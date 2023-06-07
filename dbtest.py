# Module Imports
import random
import time
from time import sleep

import pandas as pd
import tqdm
import MySQLdb as mysql


def normalJob(seed):
    work()
    timestamps.append(time.time())
    status.append("S")
    try:
        conn = mysql.connect(user="root",
                             password="pass",
                             host="192.168.1.60",
                             port=3306,
                             database="employees")
    except mysql.Error as e:
        print(f"Error connecting to mysql Platform: {e}")
        return

    data = "('1961-05-02', 'Waiman', 'Attimonelli' , 'F', '1996-11-11'),('1961-05-04', 'Sample', 'SampleLast' , 'M', " \
           "'1996-11-11')"
    full_data = ""

    rand = random.randint(10000, 50000)

    for i in range(1, rand):
        if i < rand - 1:
            full_data += data + ","
        else:
            full_data += data

    # Get Cursor
    cur = conn.cursor()

    if seed == 0:
        seed = random.randint(0, 2)
        if seed == 0:
            cur.execute("select * from employees where gender = 'M' ORDER BY emp_no DESC LIMIT 50000;")
        elif seed == 1:
            cur.execute("select * from employees where gender = 'F' ORDER BY emp_no DESC LIMIT 65000;")
        elif seed == 2:
            cur.execute(
                "select last_name, emp_no from employees  where emp_no % 3 = 0 ORDER BY emp_no DESC LIMIT 78000;")
    elif seed == 1:
        cur.execute(
            "insert into employees (birth_date, first_name, last_name, gender, hire_date) values {0};".format(
                full_data))
    elif seed == 2 or seed == 3:
        cur.execute("DELETE FROM employees WHERE emp_no > 500000 ORDER BY emp_no DESC LIMIT {0};".format(rand))
    elif seed == 4:
        cur.execute("DELETE FROM employees WHERE emp_no > 500000;")
    print("going fine")


def buggyJob(seed):
    work()
    timestamps.append(time.time())
    status.append("F")
    if seed == 0:
        try:
            conn = mysql.connect(user="root",
                                 password="pass",
                                 host="192.168.1.60",
                                 port=3306,
                                 database="employees")

            # Get Cursor
            cur = conn.cursor()

            cur.execute("SELECT first_name, last_name FROM wrongTable")
        except mysql.Error as e:
            print(f"ERROR!: {e}")
            sleep(3)
    elif seed == 1:
        try:
            conn = mysql.connect(user="root",
                                 password="pass",
                                 host="192.168.1.60",
                                 port=3306,
                                 database="employees")

            # Get Cursor
            cur = conn.cursor()

            cur.execute("SELECT first_name, wrongColumn FROM employees")
        except mysql.Error as e:
            print(f"ERROR!: {e}")
            sleep(3)
    elif seed == 2:
        try:
            conn = mysql.connect(user="root",
                                 password="pass",
                                 host="192.168.1.60",
                                 port=3306,
                                 database="employees")

            # Get Cursor
            cur = conn.cursor()

            cur.execute("SELECT * FROM employees where")
        except mysql.Error as e:
            print(f"ERROR!: {e}")
            sleep(3)


def work():
    dummy = 0
    print("WORKING...")
    for _ in tqdm.tqdm(range(0, 5000000)):
        dummy = dummy * 5
    print("Done working, querying now...")


counter = 0
timestamps = []

status = []

# print(time.time())

#
# timeSwitch = time.time() + 60 * 5
# while time.time() <= timeSwitch:
#     normalJob(random.randint(0, 9))
#
#
# print("Switching")

t_end = time.time() + 60 * 60 * 10
# cleanup_time = time.time() + 7200
cleanup_time = time.time() + 80

frame = 0
while time.time() < t_end:
    if time.time() - 120 < cleanup_time < time.time() + 120:
        cleanup_time += 7200
        normalJob(4)
    elif random.randint(0, 9) < 4:
        # if frame == 2:
        buggyJob(random.randint(0, 2))
        # After an anomaly the program sleeps for 40 seconds as a means of recovery

        sleep(40)
        counter += 1
    else:
        normalJob(random.randint(0, 4))

dict = {
    "timestamp": timestamps,
    "status": status
}

df = pd.DataFrame(dict)

df.to_csv("results.csv")
