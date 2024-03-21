from sqlalchemy import String, JSON, create_engine
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column


class Base(DeclarativeBase):
    pass


class Event(Base):
    __tablename__ = "event"
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    type: Mapped[str] = mapped_column(String(100))
    topic: Mapped[str] = mapped_column(String(100))
    message: Mapped[str] = mapped_column(JSON())

    def __repr__(self) -> str:
        return f"Event(id={self.id!r}, type={self.type!r}) topic={self.topic!r})"


if __name__ == "__main__":
    engine = create_engine("sqlite:///local.db", echo=True)
    Base.metadata.create_all(engine)
