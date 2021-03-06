# vim: set fileencoding=utf-8 sw=4 ts=4 et :
# bedup - Btrfs deduplication
# Copyright (C) 2012 Gabriel de Perthuis <g2p.code+bedup@gmail.com>
#
# This file is part of bedup.
#
# bedup is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 2 of the License, or
# (at your option) any later version.
#
# bedup is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with bedup.  If not, see <http://www.gnu.org/licenses/>.

import collections
import errno
import fcntl
import gc
import hashlib
import os
import re
import resource
import stat
import subprocess
import sys

from contextlib import closing
from contextlib2 import ExitStack
from sqlalchemy import and_

from .btrfs import (
    lookup_ino_path_one, get_fsid, get_root_id,
    get_root_generation, clone_data, defragment,
    BTRFS_FIRST_FREE_OBJECTID)
from .datetime import system_now
from .dedup import ImmutableFDs, cmp_files
from .openat import fopenat, fopenat_rw
from .model import (
    Filesystem, Volume, Inode, comm_mappings, get_or_create,
    DedupEvent, DedupEventInode, VolumePathHistory)
from sqlalchemy.sql import func, literal_column

BUFSIZE = 8192

WINDOW_SIZE = 1024

FS_ENCODING = sys.getfilesystemencoding()

# 32MiB, initial scan takes about 12', might gain 15837689948,
# sqlite takes 256k
DEFAULT_SIZE_CUTOFF = 32 * 1024 ** 2
# about 12' again, might gain 25807974687
DEFAULT_SIZE_CUTOFF = 16 * 1024 ** 2
# 13'40" (36' with a backup job running in parallel), might gain 26929240347,
# sqlite takes 758k
DEFAULT_SIZE_CUTOFF = 8 * 1024 ** 2


def get_vol(sess, volpath, size_cutoff):
    volpath = os.path.normpath(volpath)
    volume_fd = os.open(volpath, os.O_DIRECTORY)
    fs, fs_created = get_or_create(
        sess, Filesystem,
        uuid=str(get_fsid(volume_fd)))
    vol, vol_created = get_or_create(
        sess, Volume,
        fs=fs, root_id=get_root_id(volume_fd))

    if size_cutoff is not None:
        vol.size_cutoff = size_cutoff
    elif vol_created:
        vol.size_cutoff = DEFAULT_SIZE_CUTOFF

    path_history, ph_created = get_or_create(
        sess, VolumePathHistory, vol=vol, path=volpath)

    # If a volume was given multiple times on the command line,
    # keep the first name and fd for it.
    if hasattr(vol, 'fd'):
        os.close(volume_fd)
    else:
        vol.fd = volume_fd
        vol.st_dev = os.fstat(volume_fd).st_dev
        # Only use the path as a description, it is liable to change.
        vol.desc = volpath
    return vol


def forget_vol(sess, vol):
    # Forgets Inodes, not logging. Make that configurable?
    sess.query(Inode).filter_by(vol=vol).delete()
    vol.last_tracked_generation = 0
    sess.commit()


BLKID_RE = re.compile(
    br'^(?P<dev>/dev/[^:]*): '
    br'(?:LABEL="(?P<label>[^"]*)" )?UUID="(?P<uuid>[^"]*)"\s*$')


def parse_btrfs_mountinfo():
    mpoints_by_dev = collections.defaultdict(list)
    with open('/proc/self/mountinfo') as mounts:
        for line in mounts:
            items = line.split()
            idx = items.index('-')
            fs_type = items[idx + 1]
            if fs_type != 'btrfs':
                continue
            volpath = items[3]
            mpoint = items[4]
            dev = os.path.realpath(items[idx + 2])
            mpoints_by_dev[dev].append((volpath, mpoint))
    return mpoints_by_dev


def is_subvolume(btrfs_mountpoint_fd):
    st = os.fstat(btrfs_mountpoint_fd)
    return st.st_ino == BTRFS_FIRST_FREE_OBJECTID


def show_fs(fs, mpoints_by_root_id, initial_indent, indent):
    for vol in fs.volumes:
        sys.stdout.write(
            initial_indent +
            'Volume %d last tracked generation %d size cutoff %d\n'
            % (vol.root_id, vol.last_tracked_generation,
               vol.size_cutoff))
        sys.stdout.write(
            initial_indent + indent +
            '%d inodes tracked\n' % (vol.inode_count, ))

        if vol.root_id in mpoints_by_root_id:
            for (volpath, mpoint) in mpoints_by_root_id[vol.root_id]:
                sys.stdout.write(
                    initial_indent + indent + 'Mounted on %s\n' % mpoint)
            # volpath should be the same for all mpoints
            sys.stdout.write(initial_indent + indent + 'Path %s\n' % volpath)
        else:
            sys.stdout.write(
                initial_indent + indent + 'Last mounted on %s\n'
                % vol.last_known_mountpoint)


def show_vols(sess):
    mpoints_by_dev = parse_btrfs_mountinfo()

    def get_subvol_mpoints_by_root_id(dev):
        # Tends to be a less descriptive name, so keep the original
        # name blkid gave for printing.
        dev_canonical = os.path.realpath(dev)

        if dev_canonical not in mpoints_by_dev:
            return {}

        mpoints_by_root_id = collections.defaultdict(list)
        for volpath, mpoint in mpoints_by_dev[dev_canonical]:
            mpoint_fd = os.open(mpoint, os.O_DIRECTORY)
            if not is_subvolume(mpoint_fd):
                continue
            try:
                root_id = get_root_id(mpoint_fd)
                # Would help us show the volume path, by:
                # 1. finding the volume in the tree of tree roots
                # 2. finding the backref in the volume it points to
                # Part 2. would require making our own private
                # subvol=/ mount, existing mounts may not be suitable.
                #volumes_from_root_tree(mpoint_fd)
            except IOError as e:
                if e.errno == errno.EPERM:
                    break
                raise
            mpoints_by_root_id[root_id].append((volpath, mpoint))
        return mpoints_by_root_id

    seen_fs_ids = []
    for line in subprocess.check_output(
        'blkid -s LABEL -s UUID -t TYPE=btrfs'.split()
    ).splitlines():
        dev, label, uuid = BLKID_RE.match(line).groups()
        sys.stdout.write('%s\n  Label: %s UUID: %s\n' % (dev, label, uuid))
        fs = sess.query(Filesystem).filter_by(uuid=uuid).scalar()
        if fs is not None:
            seen_fs_ids.append(fs.id)
            mpoints_by_root_id = get_subvol_mpoints_by_root_id(dev)
            show_fs(fs, mpoints_by_root_id, '    ', '  ')

    query = sess.query(Filesystem)
    if seen_fs_ids:
        query = query.filter(~ Filesystem.id.in_(seen_fs_ids))
    for fs in query:
        sys.stdout.write('<device unavailable>\n  UUID: %s\n' % (fs.uuid,))
        show_fs(fs, {}, '    ', '  ')

    sess.commit()


def track_updated_files(sess, vol, tt):
    from .btrfs import ffi, u64_max

    top_generation = get_root_generation(vol.fd)
    if (vol.last_tracked_size_cutoff is not None
        and vol.last_tracked_size_cutoff <= vol.size_cutoff):
        min_generation = vol.last_tracked_generation + 1
    else:
        min_generation = 0
    tt.notify(
        'Scanning volume %r generations from %d to %d, with size cutoff %d'
        % (vol.desc, min_generation, top_generation, vol.size_cutoff))
    if min_generation > top_generation:
        tt.notify('Generation didn\'t change, skipping scan')
        sess.commit()
        return
    tt.format(
        '{elapsed} Updated {desc:counter} items: '
        '{path:truncate-left} {desc}')

    args = ffi.new('struct btrfs_ioctl_search_args *')
    args_buffer = ffi.buffer(args)
    sk = args.key
    lib = ffi.verifier.load_library()

    # Not a valid objectid that I know.
    # But find-new uses that and it seems to work.
    sk.tree_id = 0

    # Because we don't have min_objectid = max_objectid,
    # a min_type filter would be ineffective.
    # min_ criteria are modified by the kernel during tree traversal;
    # they are used as an iterator on tuple order,
    # not an intersection of min ranges.
    sk.min_transid = min_generation

    sk.max_objectid = u64_max
    sk.max_offset = u64_max
    sk.max_transid = u64_max
    sk.max_type = lib.BTRFS_INODE_ITEM_KEY

    while True:
        sk.nr_items = 4096

        try:
            fcntl.ioctl(
                vol.fd, lib.BTRFS_IOC_TREE_SEARCH, args_buffer)
        except IOError:
            raise

        if sk.nr_items == 0:
            break

        offset = 0
        for item_id in xrange(sk.nr_items):
            sh = ffi.cast(
                'struct btrfs_ioctl_search_header *', args.buf + offset)
            offset += ffi.sizeof('struct btrfs_ioctl_search_header') + sh.len

            # We can't prevent the search from grabbing irrelevant types
            if sh.type == lib.BTRFS_INODE_ITEM_KEY:
                item = ffi.cast(
                    'struct btrfs_inode_item *', sh + 1)
                inode_gen = lib.btrfs_stack_inode_generation(item)
                size = lib.btrfs_stack_inode_size(item)
                mode = lib.btrfs_stack_inode_mode(item)
                if size < vol.size_cutoff:
                    continue
                # XXX Should I use inner or outer gen in these checks?
                # Inner gen seems to miss updates (due to delalloc?),
                # whereas outer gen has too many spurious updates.
                if (vol.last_tracked_size_cutoff
                    and size >= vol.last_tracked_size_cutoff):
                    if inode_gen <= vol.last_tracked_generation:
                        continue
                else:
                    if inode_gen < min_generation:
                        continue
                if not stat.S_ISREG(mode):
                    continue
                ino = sh.objectid
                inode, inode_created = get_or_create(
                    sess, Inode, vol=vol, ino=ino)
                inode.size = size
                inode.has_updates = True

                try:
                    path = lookup_ino_path_one(vol.fd, ino)
                except IOError as e:
                    tt.notify(
                        'Error at path lookup of inode %d: %r' % (ino, e))
                    if inode_created:
                        sess.expunge(inode)
                    else:
                        sess.delete(inode)
                    continue

                try:
                    path = path.decode(FS_ENCODING)
                except ValueError:
                    continue
                tt.update(path=path)
                tt.update(
                    desc='(ino %d outer gen %d inner gen %d size %d)' % (
                        ino, sh.transid, inode_gen, size))
        sk.min_objectid = sh.objectid
        sk.min_type = sh.type
        sk.min_offset = sh.offset

        sk.min_offset += 1
    vol.last_tracked_generation = top_generation
    vol.last_tracked_size_cutoff = vol.size_cutoff
    sess.commit()


def windowed_query(window_start, query, attr, per, clear_updates):
    # [window_start, window_end] is inclusive at both ends
    # Figure out how to use attr for property access as well?
    query = query.order_by(-attr)

    while True:
        li = query.filter(attr <= window_start).limit(per).all()
        if not li:
            clear_updates(window_start, 0)
            return
        for el in li:
            yield el
        window_end = el.size
        clear_updates(window_start, window_end)
        window_start = window_end - 1


def windowed_query(window_start, query, attr, per):
    # [window_start, window_end] is inclusive at both ends
    # Figure out how to use attr for property access as well?
    query = query.order_by(-attr)

    while True:
        li = query.filter(attr <= window_start).limit(per).all()
        if not li:
            return
        for el in li:
            yield el
        window_end = el.size
        window_start = window_end - 1


def dedup_tracked(sess, volset, tt):
    skipped = []
    fs = volset[0].fs
    vol_ids = [vol.id for vol in volset]
    assert all(vol.fs == fs for vol in volset)

    # 3 for stdio, 3 for sqlite (wal mode), 1 that somehow doesn't
    # get closed, 1 per volume.
    ofile_reserved = 7 + len(volset)

    FilteredInode, Commonality1 = comm_mappings(fs.id, vol_ids)
    query = sess.query(Commonality1)
    le = query.count()

    def clear_updates(window_start, window_end):
        # Can't call update directly on FilteredInode because it is aliased.
        sess.execute(
            Inode.__table__.update().where(and_(
                Inode.vol_id.in_(vol_ids),
                window_start >= Inode.size >= window_end
            )).values(
                has_updates=False))

        for inode in skipped:
            inode.has_updates = True
        sess.commit()
        # clear the list
        skipped[:] = []

    if le:
        tt.format('{elapsed} Size group {comm1:counter}/{comm1:total}')
        tt.set_total(comm1=le)

        # This is higher than query.first().size, and will also clear updates
        # without commonality.
        window_start = sess.query(Inode).order_by(-Inode.size).first().size

        query = windowed_query(
            window_start, query, attr=Commonality1.size, per=WINDOW_SIZE,
            clear_updates=clear_updates)
        dedup_tracked1(sess, tt, ofile_reserved, query, fs, skipped)

    sess.commit()


def dedup_tracked1(sess, tt, ofile_reserved, query, fs, skipped):
    space_gain1 = space_gain2 = space_gain3 = 0
    ofile_soft, ofile_hard = resource.getrlimit(resource.RLIMIT_OFILE)

    # Hopefully close any files we left around
    gc.collect()

    # The log can cause frequent commits, we don't mind losing them in
    # a crash (no need for durability). SQLite is in WAL mode, so this pragma
    # should disable most commit-time fsync calls without compromising
    # consistency.
    sess.execute('PRAGMA synchronous=NORMAL;')

    for comm1 in query:
        if len(sess.identity_map) > 300:
            sess.flush()

        space_gain1 += comm1.size * (comm1.inode_count - 1)
        tt.update(comm1=comm1)
        for inode in comm1.inodes:
            # XXX Need to cope with deleted inodes.
            # We cannot find them in the search-new pass, not without doing
            # some tracking of directory modifications to poke updated
            # directories to find removed elements.

            # rehash everytime for now
            # I don't know enough about how inode transaction numbers are
            # updated (as opposed to extent updates) to be able to actually
            # cache the result
            try:
                path = lookup_ino_path_one(inode.vol.fd, inode.ino)
            except IOError as e:
                if e.errno != errno.ENOENT:
                    raise
                # We have a stale record for a removed inode
                # XXX If an inode number is reused and the second instance
                # is below the size cutoff, we won't update the .size
                # attribute and we won't get an IOError to notify us
                # either.  Inode reuse does happen (with and without
                # inode_cache), so this branch isn't enough to rid us of
                # all stale entries.  We can also get into trouble with
                # regular file inodes being replaced by some other kind of
                # inode.
                sess.delete(inode)
                continue
            with closing(fopenat(inode.vol.fd, path)) as rfile:
                inode.mini_hash_from_file(rfile)

        for comm2 in comm1.comm2:
            space_gain2 += comm2.size * (comm2.inode_count - 1)
            tt.update(comm2=comm2)
            for inode in comm2.inodes:
                try:
                    path = lookup_ino_path_one(inode.vol.fd, inode.ino)
                except IOError as e:
                    if e.errno != errno.ENOENT:
                        raise
                    sess.delete(inode)
                    continue
                with closing(fopenat(inode.vol.fd, path)) as rfile:
                    inode.fiemap_hash_from_file(rfile)

            if not comm2.comm3:
                continue

            comm3, = comm2.comm3
            count3 = comm3.inode_count
            space_gain3 += comm3.size * (count3 - 1)
            tt.update(comm3=comm3)
            files = []
            fds = []
            fd_names = {}
            fd_inodes = {}
            by_hash = collections.defaultdict(list)

            # XXX I have no justification for doubling count3
            ofile_req = 2 * count3 + ofile_reserved
            if ofile_req > ofile_soft:
                if ofile_req <= ofile_hard:
                    resource.setrlimit(
                        resource.RLIMIT_OFILE, (ofile_req, ofile_hard))
                    ofile_soft = ofile_req
                else:
                    tt.notify(
                        'Too many duplicates (%d at size %d), '
                        'would bring us over the open files limit (%d, %d).'
                        % (count3, comm3.size, ofile_soft, ofile_hard))
                    for inode in comm3.inodes:
                        if inode.has_updates:
                            skipped.append(inode)
                    continue

            for inode in comm3.inodes:
                # Open everything rw, we can't pick one for the source side
                # yet because the crypto hash might eliminate it.
                # We may also want to defragment the source.
                try:
                    path = lookup_ino_path_one(inode.vol.fd, inode.ino)
                except IOError as e:
                    if e.errno == errno.ENOENT:
                        sess.delete(inode)
                        continue
                    raise
                try:
                    afile = fopenat_rw(inode.vol.fd, path)
                except IOError as e:
                    if e.errno == errno.ETXTBSY:
                        # The file contains the image of a running process,
                        # we can't open it in write mode.
                        tt.notify('File %r is busy, skipping' % path)
                        skipped.append(inode)
                        continue
                    elif e.errno == errno.EACCES:
                        # Could be SELinux or immutability
                        tt.notify('Access denied on %r, skipping' % path)
                        skipped.append(inode)
                        continue
                    elif e.errno == errno.ENOENT:
                        # The file was moved or unlinked by a racing process
                        tt.notify('File %r may have moved, skipping' % path)
                        skipped.append(inode)
                        continue
                    raise

                # It's not completely guaranteed we have the right inode,
                # there may still be race conditions at this point.
                # Gets re-checked below (tell and fstat).
                fd = afile.fileno()
                fd_inodes[fd] = inode
                fd_names[fd] = path
                files.append(afile)
                fds.append(fd)

            with ExitStack() as stack:
                for afile in files:
                    stack.enter_context(closing(afile))
                # Enter this context last
                immutability = stack.enter_context(ImmutableFDs(fds))

                for afile in files:
                    fd = afile.fileno()
                    inode = fd_inodes[fd]
                    if fd in immutability.fds_in_write_use:
                        tt.notify('File %r is in use, skipping' % fd_names[fd])
                        skipped.append(inode)
                        continue
                    hasher = hashlib.sha1()
                    for buf in iter(lambda: afile.read(BUFSIZE), b''):
                        hasher.update(buf)

                    # Gets rid of a race condition
                    st = os.fstat(fd)
                    if st.st_ino != inode.ino:
                        skipped.append(inode)
                        continue
                    if st.st_dev != inode.vol.st_dev:
                        skipped.append(inode)
                        continue

                    size = afile.tell()
                    if size != comm3.size:
                        if size < inode.vol.size_cutoff:
                            # if we didn't delete this inode, it would cause
                            # spurious comm groups in all future invocations.
                            sess.delete(inode)
                        else:
                            skipped.append(inode)
                        continue

                    by_hash[hasher.digest()].append(afile)

                for fileset in by_hash.itervalues():
                    if len(fileset) < 2:
                        continue
                    sfile = fileset[0]
                    sfd = sfile.fileno()
                    # Commented out, defragmentation can unshare extents.
                    # It can also disable compression as a side-effect.
                    if False:
                        defragment(sfd)
                    dfiles = fileset[1:]
                    dfiles_successful = []
                    for dfile in dfiles:
                        dfd = dfile.fileno()
                        sname = fd_names[sfd]
                        dname = fd_names[dfd]
                        if not cmp_files(sfile, dfile):
                            # Probably a bug since we just used a crypto hash
                            tt.notify('Files differ: %r %r' % (sname, dname))
                            assert False, (sname, dname)
                            continue
                        if clone_data(dest=dfd, src=sfd, check_first=True):
                            tt.notify('Deduplicated: %r %r' % (sname, dname))
                            dfiles_successful.append(dfile)
                        else:
                            tt.notify(
                                'Did not deduplicate (same extents): %r %r' % (
                                    sname, dname))
                    if dfiles_successful:
                        evt = DedupEvent(
                            fs=fs, item_size=comm3.size, created=system_now())
                        sess.add(evt)
                        for afile in [sfile] + dfiles_successful:
                            inode = fd_inodes[afile.fileno()]
                            evti = DedupEventInode(
                                event=evt, ino=inode.ino, vol=inode.vol)
                            sess.add(evti)
                        sess.commit()

    tt.format(None)
    tt.notify(
        'Potential space gain: pass 1 %d, pass 2 %d pass 3 %d' % (
            space_gain1, space_gain2, space_gain3))
    # Restore fsync so that the final commit (in dedup_tracked)
    # will be durable.
    sess.commit()
    sess.execute('PRAGMA synchronous=FULL;')


ofile_soft = 0
ofile_hard = 0
ofile_reserved = 0
fs = 0

def dedup_tracked2(sess, volset, tt):
    global ofile_soft
    global ofile_hard
    global ofile_reserved
    global fs

    space_gain1 = space_gain2 = space_gain3 = 0
    vol_ids = [vol.id for vol in volset]
    fs = volset[0].fs
    assert all(vol.fs == fs for vol in volset)

    ofile_soft, ofile_hard = resource.getrlimit(resource.RLIMIT_OFILE)

    # 3 for stdio, 3 for sqlite (wal mode), 1 that somehow doesn't
    # get closed, 1 per volume.
    ofile_reserved = 7 + len(volset)

    try:
        tt.format('{elapsed} Size group {comm1:counter}/{comm1:total}')

        groups = sess.query(
            Inode.size,
            func.count().label('inode_count'),
            func.max(Inode.has_updates).label('has_updates'),
        ).filter(and_(
            Inode.vol_id.in_(vol_ids),
            Inode.fs_id == fs.id,
        )).group_by(
            -Inode.size
        ).having(and_(
            literal_column('inode_count') > 1,
            literal_column('has_updates') > 0,
        )).all()

        tt.set_total(comm1=len(groups))

        for group in groups[50000:]:
            tt.update(comm1=group)
            query = sess.query(
                Inode
            ).filter(
                Inode.vol_id.in_(vol_ids),
                Inode.fs_id == fs.id,
                Inode.size == group.size,
            ).all()

            do_hashing(sess, tt, query)


    except:
        # Empty except just so that we can have an else: branch,
        # when returning without errors.
        raise
    else:
        sess.execute(
            Inode.__table__.update().where(
                Inode.vol_id.in_(vol_ids)
            ).values(
                has_updates=False))
        for inode in skipped:
            inode.has_updates = True
        sess.commit()


def do_hashing(sess, tt, chunk):

    #print "> do hashing ", chunk[0].size, len(chunk)

    by_hash = collections.defaultdict(list)

    for inode in chunk:
        # XXX Need to cope with deleted inodes.
        # We cannot find them in the search-new pass,
        # not without doing some tracking of directory modifications to
        # poke updated directories to find removed elements.

        # rehash everytime for now
        # I don't know enough about how inode transaction numbers
        # are updated (as opposed to extent updates)
        # to be able to actually cache the result
        try:
            path = lookup_ino_path_one(inode.vol.fd, inode.ino)
        except IOError as e:
            if e.errno != errno.ENOENT:
                raise
            # We have a stale record for a removed inode
            # XXX If an inode number is reused and the second instance
            # is below the size cutoff, we won't update the .size
            # attribute and we won't get an IOError to notify us
            # either.  Inode reuse does happen (with and without
            # inode_cache), so this branch isn't enough to rid us of
            # all stale entries.  We can also get into trouble with
            # regular file inodes being replaced by some other kind of
            # inode.
            sess.delete(inode)
            #HR: Delete from chunk
            continue
        rfile = fopenat(inode.vol.fd, path)
        inode.mini_hash_from_file(rfile)
        rfile.close()
        by_hash[inode.mini_hash].append(inode)

    for newChunk in by_hash.itervalues():
        if len(newChunk) > 1:
            do_hashing2(sess, tt, newChunk)


def do_hashing2(sess, tt, chunk):

    #print ">> do hashing2 ", chunk[0].size, chunk[0].mini_hash, len(chunk)

    seen = {}
    for inode in chunk:
        try:
            path = lookup_ino_path_one(inode.vol.fd, inode.ino)
        except IOError as e:
            if e.errno != errno.ENOENT:
                raise
            sess.delete(inode)
            #HR: Delete from chunk
            continue
        rfile = fopenat(inode.vol.fd, path)
        inode.fiemap_hash_from_file(rfile)
        rfile.close()

        if inode.mini_hash not in seen:
            seen[inode.fiemap_hash] = inode

    chunk[:] = seen.values()

    #print ">> end hashing2 ", len(chunk)

    if len(chunk) > 1:
        do_dedup(sess, tt, chunk)


def do_dedup(sess, tt, chunk):

    #print ">>> do dedup ", chunk[0].size, chunk[0].mini_hash, len(chunk)

    global ofile_soft
    global ofile_hard
    global ofile_reserved
    global fs

    files = []
    fds = []
    fd_names = {}
    fd_inodes = {}
    by_hash = collections.defaultdict(list)

    # XXX I have no justification for doubling count3
    ofile_req = 2 * len(chunk) + ofile_reserved
    if ofile_req > ofile_soft:
        if ofile_req <= ofile_hard:
            resource.setrlimit(
                resource.RLIMIT_OFILE, (ofile_req, ofile_hard))
            ofile_soft = ofile_req
        else:
            tt.notify(
                'Too many duplicates (%d at size %d), '
                'would bring us over the open files limit (%d, %d).'
                % (count3, comm3.size, ofile_soft, ofile_hard))
            for inode in comm3.inodes:
                if inode.has_updates:
                    skipped.append(inode)
                    continue

    for inode in chunk:
        # Open everything rw, we can't pick one for the source side
        # yet because the crypto hash might eliminate it.
        # We may also want to defragment the source.
        try:
            path = lookup_ino_path_one(inode.vol.fd, inode.ino)
        except IOError as e:
            if e.errno == errno.ENOENT:
                sess.delete(inode)
                continue
            raise
        try:
            afile = fopenat_rw(inode.vol.fd, path)
        except IOError as e:
            if e.errno == errno.ETXTBSY:
                # The file contains the image of a running process,
                # we can't open it in write mode.
                tt.notify('File %r is busy, skipping' % path)
                skipped.append(inode)
                continue
            elif e.errno == errno.EACCES:
                # Could be SELinux or immutability
                tt.notify('Access denied on %r, skipping' % path)
                skipped.append(inode)
                continue
            elif e.errno == errno.ENOENT:
                # The file was moved or unlinked by a racing process
                tt.notify('File %r may have moved, skipping' % path)
                skipped.append(inode)
                continue
            raise

        # It's not completely guaranteed we have the right inode,
        # there may still be race conditions at this point.
        # Gets re-checked below (tell and fstat).
        fd = afile.fileno()
        fd_inodes[fd] = inode
        fd_names[fd] = path
        files.append(afile)
        fds.append(fd)

    with ExitStack() as stack:
        for afile in files:
            stack.enter_context(closing(afile))
        # Enter this context last
        immutability = stack.enter_context(ImmutableFDs(fds))

        for afile in files:
            fd = afile.fileno()
            inode = fd_inodes[fd]
            if fd in immutability.fds_in_write_use:
                tt.notify('File %r is in use, skipping' % fd_names[fd])
                skipped.append(inode)
                continue
            hasher = hashlib.sha1()
            for buf in iter(lambda: afile.read(BUFSIZE), b''):
                hasher.update(buf)

            # Gets rid of a race condition
            st = os.fstat(fd)
            if st.st_ino != inode.ino:
                skipped.append(inode)
                continue
            if st.st_dev != inode.vol.st_dev:
                skipped.append(inode)
                continue

            size = afile.tell()
            if size != inode.size:
                if size < inode.vol.size_cutoff:
                    # if we didn't delete this inode, it would cause
                    # spurious comm groups in all future invocations.
                    sess.delete(inode)
                else:
                    skipped.append(inode)
                continue

            by_hash[hasher.digest()].append(afile)

        for fileset in by_hash.itervalues():
            if len(fileset) < 2:
                continue
            sfile = fileset[0]
            sfd = sfile.fileno()
            # Commented out, defragmentation can unshare extents.
            # It can also disable compression as a side-effect.
            if False:
                defragment(sfd)
            dfiles = fileset[1:]
            dfiles_successful = []
            for dfile in dfiles:
                dfd = dfile.fileno()
                sname = fd_names[sfd]
                dname = fd_names[dfd]
                if not cmp_files(sfile, dfile):
                    # Probably a bug since we just used a crypto hash
                    tt.notify('Files differ: %r %r' % (sname, dname))
                    assert False, (sname, dname)
                    continue
                if clone_data(dest=dfd, src=sfd, check_first=True):
                    tt.notify('Deduplicated: %r %r' % (sname, dname))
                    dfiles_successful.append(dfile)
                else:
                    tt.notify(
                        'Did not deduplicate (same extents): %r %i %r %i' % (
                            sname, fd_inodes[sfd].ino, dname, fd_inodes[dfd].ino))
            if dfiles_successful:
                evt = DedupEvent(
                    fs=fs, item_size=inode.size, created=system_now())
                sess.add(evt)
                for afile in [sfile] + dfiles_successful:
                    inode = fd_inodes[afile.fileno()]
                    evti = DedupEventInode(
                        event=evt, ino=inode.ino, vol=inode.vol)
                    sess.add(evti)
                sess.commit()

