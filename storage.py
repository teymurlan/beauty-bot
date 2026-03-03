import sqlite3
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, List


@dataclass
class User:
    tg_id: int
    username: str
    full_name: str
    phone: str
    created_at: str


@dataclass
class Booking:
    id: int
    user_id: int
    service_key: str
    service_title: str
    price: int
    book_date: str   # YYYY-MM-DD
    book_time: str   # HH:MM
    comment: str
    status: str      # pending/confirmed/cancelled
    created_at: str
    reminder_sent: int  # 0/1


class Storage:
    def __init__(self, path: str = "data.sqlite3"):
        self.path = path
        self._init_db()

    def _conn(self):
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self):
        with self._conn() as c:
            c.execute("""
            CREATE TABLE IF NOT EXISTS users(
                tg_id INTEGER PRIMARY KEY,
                username TEXT DEFAULT '',
                full_name TEXT NOT NULL,
                phone TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
            """)
            c.execute("""
            CREATE TABLE IF NOT EXISTS bookings(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                service_key TEXT NOT NULL,
                service_title TEXT NOT NULL,
                price INTEGER NOT NULL,
                book_date TEXT NOT NULL,
                book_time TEXT NOT NULL,
                comment TEXT DEFAULT '',
                status TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
            """)
            c.execute("""
            CREATE TABLE IF NOT EXISTS blocked_slots(
                book_date TEXT NOT NULL,
                book_time TEXT NOT NULL,
                PRIMARY KEY(book_date, book_time)
            );
            """)

            # migration: add reminder_sent if missing
            cols = [r["name"] for r in c.execute("PRAGMA table_info(bookings)").fetchall()]
            if "reminder_sent" not in cols:
                c.execute("ALTER TABLE bookings ADD COLUMN reminder_sent INTEGER NOT NULL DEFAULT 0;")

    # ---------- users ----------
    def upsert_user(self, tg_id: int, username: str, full_name: str, phone: str) -> None:
        now = datetime.utcnow().isoformat(timespec="seconds")
        with self._conn() as c:
            c.execute("""
            INSERT INTO users(tg_id, username, full_name, phone, created_at)
            VALUES(?,?,?,?,?)
            ON CONFLICT(tg_id) DO UPDATE SET
                username=excluded.username,
                full_name=excluded.full_name,
                phone=excluded.phone
            """, (tg_id, username or "", full_name.strip(), phone.strip(), now))

    def get_user(self, tg_id: int) -> Optional[User]:
        with self._conn() as c:
            row = c.execute("SELECT * FROM users WHERE tg_id=?", (tg_id,)).fetchone()
            if not row:
                return None
            return User(
                tg_id=row["tg_id"],
                username=row["username"] or "",
                full_name=row["full_name"],
                phone=row["phone"],
                created_at=row["created_at"]
            )

    def delete_user(self, tg_id: int) -> None:
        with self._conn() as c:
            c.execute("DELETE FROM users WHERE tg_id=?", (tg_id,))
            c.execute("DELETE FROM bookings WHERE user_id=?", (tg_id,))

    # ---------- slots ----------
    def block_slot(self, book_date: str, book_time: str) -> None:
        with self._conn() as c:
            c.execute("INSERT OR IGNORE INTO blocked_slots(book_date, book_time) VALUES(?,?)", (book_date, book_time))

    def unblock_slot(self, book_date: str, book_time: str) -> None:
        with self._conn() as c:
            c.execute("DELETE FROM blocked_slots WHERE book_date=? AND book_time=?", (book_date, book_time))

    def list_blocked_for_day(self, book_date: str) -> List[str]:
        with self._conn() as c:
            rows = c.execute("""
                SELECT book_time FROM blocked_slots
                WHERE book_date=?
                ORDER BY book_time
            """, (book_date,)).fetchall()
            return [r["book_time"] for r in rows]

    def is_slot_blocked(self, book_date: str, book_time: str) -> bool:
        with self._conn() as c:
            row = c.execute("SELECT 1 FROM blocked_slots WHERE book_date=? AND book_time=?",
                            (book_date, book_time)).fetchone()
            return row is not None

    def is_slot_taken(self, book_date: str, book_time: str) -> bool:
        with self._conn() as c:
            row = c.execute("""
                SELECT 1 FROM bookings
                WHERE book_date=? AND book_time=? AND status IN ('pending','confirmed')
            """, (book_date, book_time)).fetchone()
            return row is not None

    # ---------- bookings ----------
    def create_booking(
        self,
        user_id: int,
        service_key: str,
        service_title: str,
        price: int,
        book_date: str,
        book_time: str,
        comment: str
    ) -> int:
        now = datetime.utcnow().isoformat(timespec="seconds")
        with self._conn() as c:
            cur = c.execute("""
            INSERT INTO bookings(user_id, service_key, service_title, price, book_date, book_time, comment, status, created_at, reminder_sent)
            VALUES(?,?,?,?,?,?,?,?,?,0)
            """, (user_id, service_key, service_title, int(price), book_date, book_time, comment or "", "pending", now))
            return int(cur.lastrowid)

    def get_booking(self, booking_id: int) -> Optional[Booking]:
        with self._conn() as c:
            row = c.execute("SELECT * FROM bookings WHERE id=?", (booking_id,)).fetchone()
            if not row:
                return None
            return self._row_to_booking(row)

    def set_booking_status(self, booking_id: int, status: str) -> None:
        with self._conn() as c:
            c.execute("UPDATE bookings SET status=? WHERE id=?", (status, booking_id))

    def mark_reminder_sent(self, booking_id: int) -> None:
        with self._conn() as c:
            c.execute("UPDATE bookings SET reminder_sent=1 WHERE id=?", (booking_id,))

    def list_user_upcoming(self, user_id: int) -> List[Booking]:
        with self._conn() as c:
            rows = c.execute("""
            SELECT * FROM bookings
            WHERE user_id=? AND status IN ('pending','confirmed')
            ORDER BY book_date, book_time
            """, (user_id,)).fetchall()
            return [self._row_to_booking(r) for r in rows]

    def list_day(self, day: str) -> List[Booking]:
        with self._conn() as c:
            rows = c.execute("""
            SELECT * FROM bookings
            WHERE book_date=? AND status IN ('pending','confirmed')
            ORDER BY book_time
            """, (day,)).fetchall()
            return [self._row_to_booking(r) for r in rows]

    def list_next(self, limit: int = 25) -> List[Booking]:
        with self._conn() as c:
            rows = c.execute("""
            SELECT * FROM bookings
            WHERE status IN ('pending','confirmed')
            ORDER BY book_date, book_time
            LIMIT ?
            """, (int(limit),)).fetchall()
            return [self._row_to_booking(r) for r in rows]

    def list_for_reminders(self) -> List[Booking]:
        with self._conn() as c:
            rows = c.execute("""
            SELECT * FROM bookings
            WHERE status IN ('pending','confirmed') AND reminder_sent=0
            ORDER BY book_date, book_time
            """).fetchall()
            return [self._row_to_booking(r) for r in rows]

    def _row_to_booking(self, row: sqlite3.Row) -> Booking:
        return Booking(
            id=row["id"],
            user_id=row["user_id"],
            service_key=row["service_key"],
            service_title=row["service_title"],
            price=row["price"],
            book_date=row["book_date"],
            book_time=row["book_time"],
            comment=row["comment"] or "",
            status=row["status"],
            created_at=row["created_at"],
            reminder_sent=int(row["reminder_sent"]) if "reminder_sent" in row.keys() else 0,
        )
