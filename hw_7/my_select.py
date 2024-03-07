from pprint import pprint

from sqlalchemy import func, desc, select, and_

from conf.models import Grade, Teacher, Student, Group, Discipline
from conf.db import session


def select_1():
    """
    SELECT
        s.id,
        s.fullname,
        ROUND(AVG(g.grade), 2) AS average_grade
    FROM grades g
    JOIN students s ON s.id = g.student_id
    GROUP BY s.id
    ORDER BY average_grade DESC
    LIMIT 5;
    """
    result = (
        session.query(
            Student.id,
            Student.fullname,
            func.round(func.avg(Grade.grade), 2).label("average_grade"),
        )
        .select_from(Grade)
        .join(Student)
        .group_by(Student.id)
        .order_by(desc("average_grade"))
        .limit(5)
        .all()
    )
    return result


def select_2(discipline_id: int):
    """
    SELECT
        d.name,
        s.id,
        s.fullname,
        ROUND(AVG(g.grade), 2) AS average_grade
    FROM grades g
    JOIN students s ON s.id = g.student_id
    JOIN disciplines d ON d.id = g.discipline_id
    where g.discipline_id = 1
    GROUP BY s.id, d.name
    ORDER BY average_grade DESC
    LIMIT 1;
    """
    result = (
        session.query(
            Discipline.name,
            Student.id,
            Student.fullname,
            func.round(func.avg(Grade.grade), 2).label("average_grade"),
        )
        .select_from(Grade)
        .join(Student)
        .join(Discipline)
        .filter(Discipline.id == discipline_id)
        .group_by(Student.id, Discipline.name)
        .order_by(desc("average_grade"))
        .limit(1)
        .all()
    )
    return result


def select_3(discipline_id: int):
    """
    SELECT
        d.name,
        gr.name,
        ROUND(AVG(g.grade), 2) AS average_grade
    FROM grades g
    JOIN students s ON s.id = g.student_id
    JOIN disciplines d ON d.id = g.discipline_id
    JOIN groups gr ON gr.id = s.group_id
    WHERE d.id = 1
    GROUP BY gr.name, d.name
    ORDER BY average_grade DESC;
    """
    result = (
        session.query(
            Discipline.name,
            Group.name,
            func.round(func.avg(Grade.grade), 2).label("average_grade"),
        )
        .select_from(Grade)
        .join(Student)
        .join(Discipline)
        .join(Group)
        .filter(Discipline.id == discipline_id)
        .group_by(Group.name, Discipline.name)
        .order_by(desc("average_grade"))
        .all()
    )
    return result


def select_4():
    """
    SELECT ROUND(AVG(g.grade), 2) AS average_grade_total
    FROM grades g;
    """
    result = (
        session.query(func.round(func.avg(Grade.grade), 2).label("average_grade_total"))
        .select_from(Grade)
        .all()
    )
    return result


def select_5(teacher_id: int):
    """
    SELECT
        t.fullname AS tf,
        d.name AS discipline
    FROM teachers t
    JOIN disciplines d ON t.id = d.teacher_id
    WHERE t.id = 4
    ORDER BY tf;
    """
    result = (
        session.query(Teacher.fullname, Discipline.name)
        .select_from(Teacher)
        .join(Discipline)
        .filter(Teacher.id == teacher_id)
        .order_by(Teacher.fullname)
        .all()
    )
    return result


def select_6(group_id: int):
    """
    SELECT
        g.name,
        s.fullname
    FROM students s
    JOIN groups g ON s.group_id = g.id
    WHERE g.id = 1
    ORDER BY s.fullname;
    """
    result = (
        session.query(Group.name, Student.fullname)
        .select_from(Student)
        .join(Group)
        .filter(Group.id == group_id)
        .order_by(Student.fullname)
        .all()
    )
    return result


def select_7(group_id: int, discipline_id: int):
    """
    SELECT
        g.name,
        s.fullname,
        d.name,
        gr.grade
    FROM grades gr
    JOIN disciplines d ON d.id = gr.discipline_id
    JOIN students s ON s.id = gr.student_id
    JOIN groups g ON s.group_id = g.id
    WHERE g.id = 1 AND d.id = 1
    ORDER BY g.name;
    """
    result = (
        session.query(Group.name, Student.fullname, Discipline.name, Grade.grade)
        .select_from(Grade)
        .join(Discipline)
        .join(Student)
        .join(Group)
        .filter(Group.id == group_id)
        .filter(Discipline.id == discipline_id)
        .order_by(Group.name)
        .all()
    )
    return result


def select_8(teacher_id: int):
    """
    SELECT
        t.fullname,
        d.name,
        ROUND(AVG(g.grade), 2) AS average_grade
    FROM grades g
    JOIN disciplines d ON d.id = g.discipline_id
    JOIN teachers t ON t.id = d.teacher_id
    WHERE t.id = 5
    GROUP BY t.id, d.id
    ORDER BY average_grade DESC;
    """
    result = (
        session.query(
            Teacher.fullname,
            Discipline.name,
            func.round(func.avg(Grade.grade), 2).label("average_grade"),
        )
        .select_from(Grade)
        .join(Discipline)
        .join(Teacher)
        .filter(Teacher.id == teacher_id)
        .group_by(Teacher.id, Discipline.id)
        .order_by(desc("average_grade"))
        .all()
    )
    return result


def select_9(student_id: int):
    """
    SELECT
        s.fullname,
        d.name
    FROM students s
    JOIN grades g ON s.id = g.student_id
    JOIN disciplines d ON d.id = g.discipline_id
    WHERE s.id = 23
    GROUP BY d.name, s.fullname
    ORDER BY d.name;
    """
    result = (
        session.query(Student.fullname, Discipline.name)
        .select_from(Student)
        .join(Grade)
        .join(Discipline)
        .filter(Student.id == student_id)
        .group_by(Discipline.name, Student.fullname)
        .order_by(Discipline.name)
        .all()
    )
    return result


def select_10(student_id: int, teacher_id: int):
    """
    SELECT
        s.fullname,
        d.name,
        t.fullname
    FROM students s
    JOIN grades gr ON s.id = gr.student_id
    JOIN disciplines d ON d.id = gr.discipline_id
    JOIN teachers t ON t.id = d.teacher_id
    WHERE s.id = 22 AND t.id = 3
    GROUP BY d.name, s.fullname, t.fullname
    ORDER BY d.name;
    """
    result = (
        session.query(Student.fullname, Discipline.name, Teacher.fullname)
        .select_from(Student)
        .join(Grade)
        .join(Discipline)
        .join(Teacher)
        .filter(Teacher.id == teacher_id)
        .filter(Student.id == student_id)
        .group_by(Discipline.name, Student.fullname, Teacher.fullname)
        .order_by(Discipline.name)
        .all()
    )
    return result


def select_11(teacher_id: int, student_id: int):
    """
    SELECT
        t.fullname,
        s.fullname,
        d.name,
        ROUND(AVG(g.grade), 2) AS average_grade
    FROM grades g
    JOIN students s ON s.id = g.student_id
    JOIN disciplines d ON d.id = g.discipline_id
    JOIN teachers t ON t.id = d.teacher_id
    WHERE t.id = 1 AND s.id = 20
    GROUP BY t.id, s.id, d.id;
    """
    result = (
        session.query(
            Teacher.fullname,
            Student.fullname,
            Discipline.name,
            func.round(func.avg(Grade.grade), 2).label("average_grade"),
        )
        .select_from(Grade)
        .join(Student)
        .join(Discipline)
        .join(Teacher)
        .filter(Student.id == student_id)
        .filter(Teacher.id == teacher_id)
        .group_by(Teacher.id, Student.id, Discipline.id)
        .all()
    )
    return result


def select_12(discipline_id: int, group_id: int):
    """
    SELECT max(grade_date)
    FROM grades g
    JOIN students s on s.id = g.student_id
    WHERE g.discipline_id = 2 and s.group_id  =3;

    SELECT s.id, s.fullname, g.grade, g.grade_date
    FROM grades g
    JOIN students s on g.student_id = s.id
    WHERE g.discipline_id = 2 and s.group_id = 3 and g.grade_date = (
        SELECT max(grade_date)
        FROM grades g2
        JOIN students s2 on s2.id=g2.student_id
        WHERE g2.discipline_id = 2 and s2.group_id = 3
    );
    """

    subquery = (
        select(func.max(Grade.grade_date))
        .join(Student)
        .filter(
            and_(Grade.discipline_id == discipline_id, Student.group_id == group_id)
        )
    ).scalar_subquery()

    result = (
        session.query(Student.id, Student.fullname, Grade.grade, Grade.grade_date)
        .select_from(Grade)
        .join(Student)
        .filter(
            and_(
                Grade.discipline_id == discipline_id,
                Student.group_id == group_id,
                Grade.grade_date == subquery,
            )
        )
        .all()
    )

    return result


if __name__ == "__main__":
    pprint(select_1())
    # pprint(select_2(1))
    # pprint(select_3(1))
    # pprint(select_4())
    # pprint(select_5(1))
    # pprint(select_6(1))
    # pprint(select_7(1, 1))
    # pprint(select_8(5))
    # pprint(select_9(23))
    # pprint(select_10(22, 3))
    # pprint(select_11(1, 20))
    # pprint(select_12(3, 1))