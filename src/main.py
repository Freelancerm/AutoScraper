from .database import DB


def main() -> None:
    db = DB()
    db.init_db()


if __name__ == "__main__":
    main()
