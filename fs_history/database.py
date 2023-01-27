from __future__ import annotations
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy import ForeignKey, types, select
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy.schema import UniqueConstraint, PrimaryKeyConstraint


from typing import Any

class Base(DeclarativeBase):
    ...

class Path(Base):
    __tablename__ = "Paths"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    path: Mapped[str] = mapped_column()
    entry: Mapped[str] = mapped_column()
    versions: Mapped[list[Version]] = relationship(back_populates="path", cascade="all, delete-orphan")

    __table_args__ = (
        UniqueConstraint('path', 'entry'),
    )

    def __repr__(self):
        return f"Path(id={self.id}, path={self.path}/{self.entry})"

class Version(Base):
    __tablename__ = "Versions"

    path_id: Mapped[int] = mapped_column(ForeignKey("Paths.id"))
    path: Mapped[Path] = relationship(back_populates="versions")
    version_no: Mapped[int] = mapped_column(default=1)
    attrs = mapped_column(types.JSON)

    __table_args__ = (
        PrimaryKeyConstraint('path_id', 'version_no'),
    )

    def __repr__(self):
        return f"Version(number={self.version_no}, path_id={self.path_id}, path={self.path.path}/{self.path.entry}, attrs={self.attrs})"

class Database:
    def __init__(self, url: str, **kwargs: Any):
        self.engine = create_async_engine(url, **kwargs)
        self.session = async_sessionmaker(self.engine)
    
    def acquire(self):
        return self.session()

    async def setup(self):
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    async def update(self, path: str, entry: str):
        async with self.acquire() as session:
            obj = Path(path=path, entry=entry)
            session.add(obj)
            await session.commit()
    
    async def select_all(self):
        async with self.acquire() as session:
            stmt = select(Path)
            return await session.execute(stmt)


    