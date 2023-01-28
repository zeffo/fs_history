from __future__ import annotations
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy import ForeignKey, types, select
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy.schema import UniqueConstraint, PrimaryKeyConstraint

from typing import Any

class Base(DeclarativeBase):
    ...

class PathModel(Base):
    __tablename__ = "Paths"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    path: Mapped[str] = mapped_column()
    entry: Mapped[str] = mapped_column()
    versions: Mapped[list[VersionModel]] = relationship(back_populates="path", cascade="all, delete-orphan")

    __table_args__ = (
        UniqueConstraint('path', 'entry'),
    )

    def __repr__(self):
        return f"Path(id={self.id}, path={self.path}/{self.entry})"

    async def getch_versions(self, *, db: Database):
        stmt = select(VersionModel).where(VersionModel.path_id == self.id)
        async with db.acquire() as session:
            res = await session.scalars(stmt)
            return res.fetchall()
            

    async def add_version(self, attrs: dict[str, Any], version_no: int | None=None, *, db: Database):
        if version_no is None:
            if versions := await self.getch_versions(db=db):
                latest = max(versions, key=lambda v: v.version_no)
                version_no = latest.version_no + 1
            else:
                version_no = 1

        obj = VersionModel(path_id=self.id, version_no=version_no, attrs=attrs)
        async with db.acquire() as session:
            session.add(obj)
            await session.commit()
        return obj

class VersionModel(Base):
    __tablename__ = "Versions"

    path_id: Mapped[int] = mapped_column(ForeignKey("Paths.id"))
    path: Mapped[PathModel] = relationship(back_populates="versions")
    version_no: Mapped[int] = mapped_column(default=1)
    attrs = mapped_column(types.JSON)

    __table_args__ = (
        PrimaryKeyConstraint('path_id', 'version_no'),
    )

    def __repr__(self):
        return f"Version(number={self.version_no}, path_id={self.path_id}, attrs={self.attrs})"

class Database:
    def __init__(self, url: str, **kwargs: Any):
        self.engine = create_async_engine(url, **kwargs)
        self.session = async_sessionmaker(self.engine, expire_on_commit=False)
    
    def acquire(self):
        return self.session()

    async def setup(self):
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    async def drop_all(self):
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.drop_all)

    async def add_path(self, path: str, entry: str):
        async with self.acquire() as session:
            obj = PathModel(path=path, entry=entry)
            session.add(obj)
            await session.commit()
        return obj

    async def add_version(self, path_id: int, attrs: dict[str, Any], version_no: int | None=None):
        obj = VersionModel(path_id=path_id, version_no=version_no, attrs=attrs)
        async with self.acquire() as session:
            session.add(obj)
            await session.commit()
        return obj

    async def select_all(self):
        async with self.acquire() as session:
            stmt = select(PathModel, VersionModel).join(VersionModel)
            return await session.execute(stmt)


    