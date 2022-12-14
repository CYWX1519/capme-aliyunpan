from aligo import *
from time import sleep, time
import os
import logging
import pymysql


class MyAligo:
    def __init__(self, dest_path):
        self.client = Aligo()
        self.path = dest_path
        self.web_root_path = "/cloudreve"
        self.check_root_folder()
        self.init_database()

    def init_database(self):
        self.operator_database()
        self.cursor.close()
        self.mysql.commit()
        self.mysql.close()

    def operator_database(self):
        try:
            logging.info("starting to connecting the database")
            self.mysql = pymysql.connect(host="127.0.0.1",
                                         user="cloudreve",
                                         password="password",  # TODO setting your password
                                         database="cloudreve_db",
                                         port=3306,
                                         charset="utf8")
            self.cursor = self.mysql.cursor()
            if self.cursor.execute("show tables;") is not 0:
                tables = self.cursor.fetchall()
                is_found = False
                for table in tables:
                    is_existed = False
                    for index in len(table):
                        if table[index] is "update_records":
                            is_existed = True
                            break
                    if is_existed:
                        is_found = True
                        break
                if not is_found:
                    logging.warning(
                        "can not found the table, creating new table...")
                    self.creata_table()
                else:
                    logging.info("table found!")
            else:
                logging.warning("table do not existed, create new table...")
                self.creata_table()
            return True
        except:
            self.mysql.close()
            return False

    def creata_table(self):
        try:
            self.cursor.execute(
                "create table update_records(id int primary key not null auto_increment, name varchar(255) not null, update_time varchar(255) not null, modified_time varchar(255) not null);")
            logging.info("table created!")
        except:
            logging.error("table created falied! Exiting...")
            exit()

    def update_file(self, path, web_path):
        file_list = os.listdir(path)
        for file in file_list:
            file_path = os.path.join(path, file)
            web_file_path = os.path.join(web_path, file)
            if os.path.isfile(file_path):
                logging.info("the file will upload name is: " + file)
                sql_script = "select * from update_records where name='" + file + "';"
                logging.debug(sql_script)
                modify_time = os.path.getmtime(file_path)
                if self.cursor.execute(sql_script) is not 0:  # record exist
                    res = self.cursor.fetchone()
                    if res[3] is not modify_time:
                        if self.upload_file(file_path, web_file_path):
                            sql_script = "update update_records set update_time=" + \
                                str(time()) + ",modified_time=" + str(modify_time) + " where name='" + file + "';"
                            logging.debug(sql_script)
                            self.cursor.execute(sql_script)
                else:  # record not exist
                    if self.upload_file(file_path, web_file_path):
                        sql_script = "insert into update_records(name,update_time,modified_time) values('" + \
                            file + "','" + str(time()) + "','" + str(modify_time) + "');"
                        logging.debug(sql_script)
                        self.cursor.execute(sql_script)
            else:
                logging.info("this is a folder, name is: " + file)
                self.update_file(file_path, web_file_path)

    def upload_file(self, local_file_path, folder_path):
        web_res = self.client.get_folder_by_path(folder_path)
        if web_res is None:
            self.client.create_folder(folder_path)
        web_res = self.client.get_folder_by_path(folder_path)
        upload_respon = self.client.upload_file(file_path=local_file_path,
                                                parent_file_id=web_res.file_id,
                                                check_name_mode="overwrite")
        if upload_respon is None:
            logging.warning("upload file failed!")
            return False
        return True

    def check_root_folder(self):
        server_list = self.client.get_file_list()
        is_root_exist = False
        for folder in server_list:
            if folder.name.__eq__("cloudreve"):
                logging.info("\"cloudreve\" folder existed!")
                return
        if not is_root_exist:
            logging.warn(
                "\"clodreve\" floder do not exist, creating new folder...")
            self.client.create_folder(self.web_root_path)
            logging.info("\"cloudreve\" folder creating succeed!")

    def waitting_next_weakup(self):
        logging.info("sleep until next update...")
        sleep(3600 * 24)

    def run(self):
        while (True):
            try:
                if self.operator_database():
                    self.update_file(self.path, self.web_root_path)
            except:
                exit()
            self.cursor.close()
            self.mysql.close()
            self.waitting_next_weakup()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        filemode='w',
                        filename='./running.log',
                        format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')
    logging.info("starting this program...")
    myAligoOper = MyAligo("/home/cywx/cloudreve") # TODO change it 
    myAligoOper.run()

