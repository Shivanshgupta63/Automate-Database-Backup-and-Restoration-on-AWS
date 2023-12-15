import subprocess
import os
import time
import json
import pymysql

timestamp = time.strftime('%Y-%m-%d-%I:%M')
DATABASE_NAME = 'rds-db'
BACKUP_DATABASE_NAME = 'students_BACKUP'
TABLE = 'student'
downloaded_file = 'download_backup.sql'

def backup(event, context):
    print("Function started")
    dbHost   = "rds-db.clruv3ebkoxu.ap-south-1.rds.amazonaws.com"
    dbUser   = "admin"
    dbPass   = "shiv12345"
    S3_BUCKET = 'mybucket123a'
    print(dbHost)

    db = pymysql.connect(host=dbHost,
                        user = dbUser,
                        password=dbPass)

    cursor = db.cursor()

    sql = f"create database IF NOT EXISTS {DATABASE_NAME}"
    cursor.execute(sql)

    cursor.execute("show databases")

    data = cursor.fetchall()

    assert DATABASE_NAME in [row[0] for row in data]

    db.select_db(DATABASE_NAME)
    sql = f''' create table if not exists {TABLE} ( id int not null auto_increment,
                                    fname text, lname text,
                                    primary key (id) )
        '''

    cursor.execute(sql)
    cursor.execute("show tables")

    assert TABLE in [row[0] for row in cursor.fetchall()]
    sql = f''' insert into {TABLE}(fname, lname) values('%s', '%s')''' % ('jai', 'mahakal')
    cursor.execute(sql)

    sql = f'''select * from {TABLE}'''
    cursor.execute(sql)
    db.commit()

    print(cursor.fetchall())

    command = "mysqldump --host %s --user %s -p%s %s | aws s3 cp - s3://%s/%s" % (
        dbHost, dbUser, dbPass, DATABASE_NAME, S3_BUCKET, DATABASE_NAME + "_" + timestamp + '.sql')
    #print (command)
    subprocess.Popen(command, shell=True).wait()
    print("MySQL backup finished")


    if not os.path.exists(f'C:\\tmp\\{downloaded_file}'):
        open(f'C:\\tmp\\{downloaded_file}', 'w').close()

    import boto3
    # Create a aws client to connecto to s3
    client = boto3.client('s3')

    client.download_file(S3_BUCKET, DATABASE_NAME + "_" + timestamp + '.sql', f'C:\\tmp\\{downloaded_file}')

    cursor.execute(f"create database if not exists {BACKUP_DATABASE_NAME}")

    command = f"mysql -h {dbHost} -u {dbUser} -p{dbPass} {BACKUP_DATABASE_NAME} < f'C:\\tmpP\\{downloaded_file}"
    print(command)
    output = subprocess.run(command, shell=True)

    if output.returncode == 0:
        print("Database restore completed successfully.")
    else:
        print(f"Database restore failed with return code {output.returncode}.")

    return "backup finished"

backup(0,0)