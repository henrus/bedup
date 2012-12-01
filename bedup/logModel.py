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

from sqlalchemy.orm import relationship, column_property
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.sql import and_, select, func, literal_column, distinct
from sqlalchemy.ext.declarative import declarative_base, declared_attr
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.types import (
    Boolean, Integer, Text, DateTime, TypeDecorator)
from sqlalchemy.schema import (
    Column, ForeignKey, UniqueConstraint, CheckConstraint)

from .datetime import UTC
from .model import FK

class SuperBase(object):
    @declared_attr
    def __tablename__(cls):
        return cls.__name__

Base = declarative_base(cls=SuperBase)


class UTCDateTime(TypeDecorator):
    impl = DateTime

    def process_bind_param(self, value, engine):
        return value.astimezone(UTC)

    def process_result_value(self, value, engine):
        return value.replace(tzinfo=UTC)


# The logging classes don't have anything in common (no FKs)
# with the tracking classes. For example, inode numbers may
# be reused, and inodes can be removed from tracking in these
# cases. That would cause dangling references or delete cascades.
# We do allow FKs to volumes; those aren't meant to be removed.
class DedupEvent(Base):
    id = Column(Integer, primary_key=True)
    fs_id = Column(Integer)

    item_size = Column(Integer, index=True, nullable=False)
    created = Column(UTCDateTime, index=True, nullable=False)

    @hybrid_property
    def estimated_space_gain(self):
        return self.item_size * (self.inode_count - 1)

    __table_args__ = (
        dict(
            sqlite_autoincrement=True))


class DedupEventInode(Base):
    id = Column(Integer, primary_key=True)
    event_id, event = FK(DedupEvent.id)
    ino = Column(Integer, index=True, nullable=False)
    vol_id = Column(Integer)

    __table_args__ = (
        dict(
            sqlite_autoincrement=True))

DedupEvent.inode_count = column_property(
    select([func.count(DedupEventInode.id)])
    .where(DedupEventInode.event_id == DedupEvent.id)
    .label('inode_count'))


LOG = Base.metadata

