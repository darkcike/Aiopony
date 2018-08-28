# Aiopony


python3.5+
Based on ponyorm.

Database adapted:

    mysql: using aiomysql.
 
### Can only suport one Database now.
 
  Useage:
  
 ```
from Aiopony import *

db = AioDatabase()


async def init_db(config):
    db_host = "127.0.0.1"
    db_user = "root"
    db_password = "123456"
    db_name = "data"
    await db.bind('mysql', host=db_host, user=db_user, passwd=db_password, db=db_name)
    await db.generate_mapping(create_tables=True)
    
class Student(db.Entity):
    _table_ = "Students"
    record = PrimaryKey(int)
    name = Required(unicode, column="fio")
    group = Required("Group")
    scholarship = Required(int, default=0)
    marks = Set("Mark")


class Group(db.Entity):
    _table_ = "Groups"
    number = PrimaryKey(str)
    department = Required(int)
    students = Set("Student")
    subjects = Set("Subject")


class Subject(db.Entity):
    _table_ = "Subjects"
    name = PrimaryKey(unicode)
    groups = Set("Group")
    marks = Set("Mark")


class Mark(db.Entity):
    _table_ = "Exams"
    student = Required(Student, column="student")
    subject = Required(Subject, column="subject")
    value = Required(int)
    PrimaryKey(student, subject)
    


@db_session()
async def populate_db():
    Physics = Subject(name='Physics')
    Chemistry = Subject(name='Chemistry')
    Math = Subject(name='Math')

    g3132 = Group(number='3132', department=33, subjects=[Physics, Math])
    g4145 = Group(number='4145', department=44, subjects=[Physics, Chemistry, Math])
    g4146 = Group(number='4146', department=44)

    s101 = Student(record=101, name='Bob', group=g4145, scholarship=0)
    s102 = Student(record=102, name='Joe', group=g4145, scholarship=800)
    s103 = Student(record=103, name='Alex', group=g4145, scholarship=0)
    s104 = Student(record=104, name='Brad', group=g3132, scholarship=500)
    s105 = Student(record=105, name='John', group=g3132, scholarship=1000)

    Mark(student=s101, subject=Physics, value=4)
    Mark(student=s101, subject=Math, value=3)
    Mark(student=s102, subject=Chemistry, value=5)
    Mark(student=s103, subject=Physics, value=2)
    Mark(student=s103, subject=Chemistry, value=4


 ```
