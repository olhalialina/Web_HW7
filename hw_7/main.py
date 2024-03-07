import argparse
from pprint import pprint

from conf.models import Teacher, Student, Discipline, Group
from conf.db import session

"""
python main.py -a create -m Teacher -n 'Pol McCartney'                 - створення вчителя
python main.py -a  list -m Teacher                                     - показати всіх вчителів
python main.py -a update -m Teacher --id 5 -n 'Августин Сімашкевич'    - оновити дані вчителя з id=5 
python main.py -a remove -m Teacher --id 6                             - видалити вчителя з id=6

"""

parser = argparse.ArgumentParser(description="Data base operations")
parser.add_argument(
    "-a", "--action", dest="action", help="Action: create/list/update/remove", type=str
)
parser.add_argument(
    "-m",
    "--model",
    dest="model",
    help="Model's name: Teacher/Group/Student/Discipline",
    type=str,
)
parser.add_argument(
    "-n",
    "--name",
    dest="name",
    help="Name of Teacher/Group/Student/Discipline",
    type=str,
)
parser.add_argument(
    "--id", dest="id", help="ID of Teacher/Group/Student/Discipline", type=str
)
args = parser.parse_args()


def create(model: str, name: str):
    if model == "Teacher":
        teacher = Teacher(fullname=name)
        session.add(teacher)
    elif model == "Group":
        group = Group(name=name)
        session.add(group)
    elif model == "Student":
        student = Student(fullname=name)
        session.add(student)
    elif model == "Discipline":
        discipline = Discipline(name=name)
        session.add(discipline)
    session.commit()


def list_(model: str):
    result = None
    if model == "Teacher":
        result = session.query(Teacher.fullname).select_from(Teacher).all()
    elif model == "Group":
        result = session.query(Group.name).select_from(Group).all()
    elif model == "Student":
        result = session.query(Student.fullname).select_from(Student).all()
    elif model == "Discipline":
        result = session.query(Discipline.name).select_from(Discipline).all()
    return result


def update(model: str, id: int, name: str):
    if model == "Teacher":
        new_name = session.get(Teacher, id)
        new_name.fullname = name
        session.add(new_name)
    elif model == "Group":
        new_name = session.get(Group, id)
        new_name.name = name
        session.add(new_name)
    elif model == "Student":
        new_name = session.get(Student, id)
        new_name.fullname = name
        session.add(new_name)
    elif model == "Discipline":
        new_name = session.get(Discipline, id)
        new_name.name = name
        session.add(new_name)
    session.commit()


def remove(model: str, id: int):
    if model == "Teacher":
        i = session.query(Teacher).filter(Teacher.id == id).one()
        session.delete(i)
    elif model == "Group":
        i = session.query(Group).filter(Group.id == id).one()
        session.delete(i)
    elif model == "Student":
        i = session.query(Student).filter(Student.id == id).one()
        session.delete(i)
    elif model == "Discipline":
        i = session.query(Discipline).filter(Discipline.id == id).one()
        session.delete(i)
    session.commit()


def main():
    if args.action == "create":
        create(args.model, args.name)
        pprint(f"{args.model}: {args.name} created!")
    elif args.action == "list":
        pprint(list_(args.model))
    elif args.action == "update":
        update(args.model, args.id, args.name)
    elif args.action == "remove":
        remove(args.model, args.id)
        pprint(f"{args.model} with ID '{args.id}' removed!")


if __name__ == "__main__":
    main()