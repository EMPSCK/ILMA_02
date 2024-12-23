import pymysql
import config
from queries import general_queries
from chairman_moves import check_list_judges
import re
import datetime
from datetime import date

async def pull_to_crew_group(user_id, groupNumber, area):
    active_comp = await general_queries.get_CompId(user_id)
    try:
        conn = pymysql.connect(
            host=config.host,
            port=3306,
            user=config.user,
            password=config.password,
            database=config.db_name,
            cursorclass=pymysql.cursors.DictCursor
        )
        with conn:
            cur = conn.cursor()
            sql = "INSERT INTO competition_group_crew (`compId`, `groupNumber`, `roundName`) VALUES (%s, %s, %s)"
            cur.execute(sql, (
                active_comp, groupNumber, area))
            conn.commit()
            return cur.lastrowid
    except:
        return -1

async def name_to_jud_id(last_name, name, compId):
    try:
        conn = pymysql.connect(
            host=config.host,
            port=3306,
            user=config.user,
            password=config.password,
            database=config.db_name,
            cursorclass=pymysql.cursors.DictCursor
        )
        with conn:
            cur = conn.cursor()
            cur.execute(f"select id from competition_judges where compId = {compId} and (lastName = '{last_name}' and firstName = '{name}')")
            ans = cur.fetchone()
            if ans is None:
                return -100
            else:
                ans = ans['id']
                if ans is None:
                    return -100
                else:
                    return ans
    except:
        return -1

async def pull_to_comp_group_jud(user_id, crew_id, area):
    active_comp = await general_queries.get_CompId(user_id)
    ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    have_gs = 0
    zgs = []
    lin = []
    gs = []
    try:
        if len(area) == 3:
            zgs = area[1].split(', ')
            lin = area[2].split(', ')
        if len(area) == 4:
            gs = area[1].split(', ')
            zgs = area[2].split(', ')
            lin = area[3].split(', ')
            have_gs = 1
        if len(area) == 2:
            lin = lin = area[1].split(', ')
        conn = pymysql.connect(
            host=config.host,
            port=3306,
            user=config.user,
            password=config.password,
            database=config.db_name,
            cursorclass=pymysql.cursors.DictCursor
        )
        with conn:
            cur = conn.cursor()
            for judIndex in range(len(zgs)):
                i = zgs[judIndex].split()
                if len(i) == 2:
                    lastname, firstname = i
                else:
                    lastname = i[0]
                    firstname = ' '.join(i[1::])
                judge_id = await name_to_jud_id(lastname, firstname, active_comp)
                ident = f'ЗГС'
                sql = "INSERT INTO competition_group_judges (`crewId`, `typeId`, `ident`, `lastName`, `firstName`, `judgeId`) VALUES (%s, %s, %s, %s, %s, %s)"
                cur.execute(sql, (crew_id, 1, ident, lastname, firstname, judge_id))
                conn.commit()


            for judIndex in range(len(lin)):
                i = lin[judIndex].split()
                if len(i) == 2:
                    lastname, firstname = i
                else:
                    lastname = i[0]
                    firstname = ' '.join(i[1::])
                judge_id = await name_to_jud_id(lastname, firstname, active_comp)
                ident = f'{ALPHABET[judIndex]}({judIndex + 1})'
                sql = "INSERT INTO competition_group_judges (`crewId`, `typeId`, `ident`, `lastName`, `firstName`, `judgeId`) VALUES (%s, %s, %s, %s, %s, %s)"
                cur.execute(sql, (crew_id, 0, ident, lastname, firstname, judge_id))
                conn.commit()

            for judIndex in range(len(gs)):
                i = gs[judIndex].split()
                if len(i) == 2:
                    lastname, firstname = i
                else:
                    lastname = i[0]
                    firstname = ' '.join(i[1::])
                judge_id = await name_to_jud_id(lastname, firstname, active_comp)
                ident = f'Гл. судья'
                sql = "INSERT INTO competition_group_judges (`crewId`, `typeId`, `ident`, `lastName`, `firstName`, `judgeId`) VALUES (%s, %s, %s, %s, %s, %s)"
                cur.execute(sql, (crew_id, 2, ident, lastname, firstname, judge_id))
                conn.commit()
            return 1
    except Exception as e:
        print(e)
        return -1