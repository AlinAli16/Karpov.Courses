from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel
from psycopg2.extras import RealDictCursor

import datetime as dt
import psycopg2

app = FastAPI()


class User(BaseModel):
    name: str
    surname: str
    age: int
    registration_date: dt.date

    # Конфигурация считывания данных
    class Config:
        orm_mode = True


class PostResponse(BaseModel):
    id: int
    text: str
    topic: str

    class Config:
        orm_mode = True


@app.get("/hello")
def say_hellp():
    return "hello, world"


@app.get("/sum")
def sum_two(a: int, b: int) -> int:
    return a + b


@app.get("/sum_date")
def sum_date(current_date: dt.date, offset: int):
    return current_date + dt.timedelta(days=offset)


@app.post("/user/validate")
def message_add_user(user: User):
    return f"Will add user: {user.name} {user.surname} with age {user.age}"


def get_db():
    # connection - это объект, который отвечает за соединение с БД
    connection = psycopg2.connect(
        database='startml',                     # database - это база данных (именно база, не СУБД)
        host='postgres.lab.karpov.courses',     # это говорит, что СУБД работает на моем компьютере
        user='robot-startml-ro',                # имя пользователя
        password='pheiph0hahj1Vaif',            # пароль
        port=6432,                              # порт не указываем, по умолчанию 5432
        cursor_factory=RealDictCursor
    )
    return connection


@app.get("/user/{id}")
def get_user_info(id: int, db=Depends(get_db)):
    with db.cursor() as cursor:
        cursor.execute(
         """
         SELECT gender, age, city 
         FROM "user" 
         WHERE id=%(user_id)s""", {"user_id": id}
        )
        result = cursor.fetchone()
    if result is None:
        raise HTTPException(404, detail="user not found")
    return result


@app.get("/post/{id}", response_model=PostResponse)
def get_topic_info(id: int, db=Depends(get_db)) -> PostResponse:
    with db.cursor() as cursor:
        cursor.execute(
            """
            SELECT id, text, topic 
            FROM "post" 
            WHERE id=%(user_id)s""",
            {"user_id": id}
        )
        result = cursor.fetchone()
    if result is None:
        raise HTTPException(404, detail="user not found")
    return PostResponse(**result)
