from .models import Base, PathModel, VersionModel

from sqlalchemy import create_engine, select, Select, func
from sqlalchemy.orm import sessionmaker, Session

from pathlib import Path
from typing import Any, Generator



class Database:
    def __init__(self, url: str, **kwargs: Any):
        self.engine = create_engine(url, **kwargs)
        self.session = sessionmaker(self.engine, expire_on_commit=False)

    def acquire(self) -> Session:
        """Creates and returns a new `Session`."""
        return self.session()
    
    def setup(self):
        """Creates the relations if they do not exist."""
        Base.metadata.create_all(self.engine)

    def drop(self):
        """Drops all relations."""
        Base.metadata.drop_all(self.engine)

    def _commit_model(self, model: PathModel | VersionModel):
        """Commits a model to the database."""
        with self.acquire() as session:
            session.add(model)
            session.commit()

    def get_path(self, parent: str | Path, name: str):
        """Returns a `PathModel` object.
        
        Parameters
        ----------
        parent: :class:`str | Path`
            The parent directory.
        name: :class:`str`
            The name of the file/directory.

        Returns
        -------
        :class:`PathModel`
        """
        return PathModel(parent=str(parent), name=name)
    
    def insert_path(self, parent: str | Path, name: str):
        """Inserts and returns a `PathModel` object.
        
        Parameters
        ----------
        parent: :class:`str | Path`
            The parent directory.
        name: :class:`str`
            The name of the file/directory.

        Returns
        -------
        :class:`PathModel`
        """
        path = self.get_path(parent, name)
        self._commit_model(path)
        return  path
    
    def get_version(self, path_id: int, version_no: int, attrs: dict[str, Any]):
        """Returns a `VersionModel` object.
        
        Parameters
        ----------
        path_id: :class:`int`
            The ID of the path this version belongs to.

        version_no: :class:`int`
            The version number.

        attrs: :class:`Json`
            A JSON serializable object.

        Returns
        -------
        :class:`VersionModel`
        """
        return VersionModel(path_id=path_id, version_no=version_no, attrs=attrs)
    
    def insert_version(self, path_id: int, version_no: int, attrs: dict[str, Any]):
        """Inserts and returns a `VersionModel` object.
        
        Parameters
        ----------
        path_id: :class:`int`
            The ID of the path this version belongs to.

        version_no: :class:`int`
            The version number.

        attrs: :class:`Json`
            A JSON serializable object.

        Returns
        -------
        :class:`VersionModel`
        """
        version = self.get_version(path_id=path_id, version_no=version_no, attrs=attrs)
        self._commit_model(version)
        return version
    
    def scalars(self, stmt: Select[Any]):
        """Execute a statement and return results as scalars.
        
        Parameters
        ----------
        stmt: :class:`Select`
        """
        with self.acquire() as session:
            res = session.scalars(stmt)
            for rec in res:
                yield rec

    def _select_gen(self, stmt: Select[Any]):
        with self.acquire() as session:
            res = session.execute(stmt)
            for rec in res:
                yield rec
    
    def select_all(self):
        """A generator that yields all records of the database."""
        stmt = select(PathModel, VersionModel).join(VersionModel, PathModel.id == VersionModel.path_id)
        return self._select_gen(stmt)
    
    def select_paths(self, parent: str | Path | None = None, name: str | None = None) -> Generator[PathModel, None, None]:
        """A generator that yields Paths."""
        stmt = select(PathModel)
        if parent:
            stmt = stmt.where(PathModel.parent == str(parent))
        
        if name:
            stmt = stmt.where(PathModel.name == name)

        return self.scalars(stmt)
    
    def select_versions(self, path_id: int | None = None, version_no: int | None = None) -> Generator[VersionModel, None, None]:
        """A generator that yields Versions."""
        stmt = select(VersionModel)
        if path_id:
            stmt = stmt.where(VersionModel.path_id == path_id)
        
        if version_no:
            stmt = stmt.where(VersionModel.version_no == version_no)

        return self.scalars(stmt)

    def upsert_version(self, path: Path, attrs: dict[str, Any]):
        """Updates a `Path` with a new version.

        Parameters
        ----------
        path: :class:`Path`
            The Path to update.

        attrs: :class:`Json`
            A JSON Serializable object.
        
        Returns
        -------
        :class:`Tuple[PathModel, VersionModel]`
            A tuple containing the Path and Version that were added to the database.
        """
        with self.acquire() as session:
            stmt = select(PathModel).where(PathModel.parent == str(path.parent) and PathModel.name == path.name)
            result = session.scalars(stmt).first()
            if not result:
                result = self.get_path(path.parent, path.name)
                session.add(result)
                session.commit()
                version = self.get_version(result.id, 1, attrs)
            else:
                latest = session.query(func.max(VersionModel.version_no)).filter_by(path_id=result.id).scalar()
                version = self.get_version(result.id, latest+1, attrs)
        
            session.add(version)
            session.commit()
            return result, version