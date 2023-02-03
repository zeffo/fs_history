from __future__ import annotations
from sqlalchemy import ForeignKey, types
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy.schema import UniqueConstraint, PrimaryKeyConstraint

__all__ = ("Base", "PathModel", "VersionModel")

class Base(DeclarativeBase):
    ...

class PathModel(Base):
    __tablename__ = "Paths"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    parent: Mapped[str] = mapped_column()
    name: Mapped[str] = mapped_column()
    versions: Mapped[list[VersionModel]] = relationship(back_populates="path", cascade="all, delete-orphan")

    __table_args__ = (
        UniqueConstraint('parent', 'name'),
    )

    def __repr__(self):
        return f"Path(id={self.id}, path={self.parent}/{self.name})"


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
    