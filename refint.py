import psycopg2
import sys

# team9

conn = psycopg2.connect(
    database="team9",
    user="cosc3303",
    host="/tmp/",
    password="team9"
)
# table name, list of PKs, list of FKs

test_string = sys.argv[1]
database = ''
err= '0.0'
if ';err=' in test_string:
    split_arg = (test_string.split('database='))
    database = split_arg[1].split(';')[0]
    temp = (split_arg[1].split(';'))
    err = temp[1].split('err=')[1]
else:
    split_arg = (test_string.split('database='))
    database = (split_arg[1])


f = open(database, "r")
refint = open("refint.sql",'w')

if f.mode == 'r':
    contents = f.readlines()

    for i in contents:
        table_info = []
        numPK = 0
        numFK = 0
        table = ""
        PKlist = []
        FKlist = []
        RTlist = []
        ogTable = False
        x = i.split(",")
        for z in x:
            for a in range(len(z)):
                if (z[a] == " " or z[a] == '(') and ogTable == False:
                    b = 0
                    while b < a:
                        table += z[b]
                        b += 1
                    table_info.append(table)
                    ogTable = True
                    #print("Table = ", table)
                elif z[a] == "p" and z[a+1] == "k" and numPK == 0:
                    a = a - 2
                    if z[a] == " ":
                        a = a - 1
                    while a >= 0:
                        if z[a] == ' ' or z[a] == '(' or a == 0:
                            if z[a] == ' ' or z[a] == '(':
                                a += 1
                            pk = ""
                            while z[a] != '(' and z[a] != ' ':
                                pk += z[a]
                                a += 1
                            break
                            #print("PK = ", pk)
                        else:
                            a = a - 1
                    PKlist.append(pk)
                    numPK = 1
                elif z[a] == "p" and z[a+1] == "k" and numPK == 1:
                    a = a-2
                    if z[a] == " ":
                        a = a - 1
                    while a >= 0:
                        if z[a] == ' ' or z[a] == '(' or a == 0:
                            if z[a] == ' ' or z[a] == '(':
                                a += 1
                            pk2 = ""
                            while z[a] != '(' and z[a] != ' ':
                                pk2 += z[a]
                                a += 1
                            break
                            # print("PK = ", pk)
                        else:
                            a = a - 1
                    PKlist.append(pk2)
                    numPK = 2
                elif z[a] == "p" and z[a+1] == "k" and numPK == 2:
                    a = a-2
                    while a >= 0:
                        if z[a] == ' ' or z[a] == '(' or a == 0:
                            if z[a] == ' ' or z[a] == '(':
                                a += 1
                            pk3 = ""
                            while z[a] != '(' and z[a] != ' ':
                                pk3 += z[a]
                                a += 1
                            break
                            # print("PK = ", pk3)
                        else:
                            a = a - 1
                    PKlist.append(pk3)
                    numPK = 3
                elif z[a] == 'f' and z[a+1] == 'k' and numFK == 0:
                    c = a+3
                    refTable = ""
                    while z[c] != '.':
                        refTable += z[c]
                        c += 1
                    RTlist.append(refTable)
                    a = c+1
                    fk = ""
                    while z[a] != ')':
                        fk += z[a]
                        a += 1
                    FKlist.append(fk)
                    numFK = 1
                elif z[a] == 'f' and z[a+1] == 'k' and numFK == 1:
                    c = a + 3
                    refTable = ""
                    while z[c] != '.':
                        refTable += z[c]
                        c += 1
                    RTlist.append(refTable)
                    a = c + 1
                    fk2 = ""
                    while z[a] != ')':
                        fk2 += z[a]
                        a += 1
                    FKlist.append(fk2)
                    numFK = 2


        table_info.append(PKlist)
        table_info.append(FKlist)
        table_info.append(RTlist)
        
        cur = conn.cursor()

        # cur.execute("create table pk_table ("          table_info[1][0]" varchar(50)

        # cur.execute("INSERT INTO qm (TableName) VALUES (T1)")

        # Testing something
        # cur.execute("insert into qm_table  (tablename,entityerrpt,referentialerrpt, ok) values ('D2', (select count  (distinct k11)/ count (k11)*1.0 from T1), 5.7,'O')")
        # conn.commit()

        cur.execute("create table pk_counts ( total decimal(10,1), dist decimal(10,1), name varchar(50))")
        refint.write("create table pk_counts ( total decimal(10,1), dist decimal(10,1), name varchar(50))\n")
        conn.commit()
        cur.execute("create table fk_counts ( total decimal(10,1), dist decimal(10,1), name varchar(50))")
        refint.write("create table fk_counts ( total decimal(10,1), dist decimal(10,1), name varchar(50))\n")
        conn.commit()

        # insert into pk_counts select count (name), count (distinct name)  , 'T1' from pk_tester; I don't think this one matters anymore

        # loops once for each PK. Puts total and distinct in columns. The min of distinct and max of total are used to find error
        #sql_command = "insert into qm_table (tablename) values (" + table_info[0] + ")"
        #cur.execute(sql_command)
        #conn.commit()
        sql_command = "insert into pk_counts (total) select count(1) from " + table_info[0]
        cur.execute(sql_command)
        refint.write(sql_command +'\n')
        conn.commit()
        sql_command = "insert into fk_counts (total) select count(1) from " + table_info[0]
        refint.write(sql_command +'\n')
        cur.execute(sql_command)
        conn.commit()
        if len(table_info[1]) == 1:
            # sql_command = "insert into pk_counts select count(" +pk +"), count (distinct "+pk+"),'"+table_info[0]+"' from "+table_info[0]
            sql_command = "insert into pk_counts (dist) select count(1) from (select distinct " + table_info[1][0] + " from " + \
                          table_info[0] + " where (" + table_info[1][0] + ") is not null) as primary_count"
            #print("SQL Command is : ", sql_command)
            cur.execute(sql_command)
            refint.write(sql_command +'\n')
            conn.commit()
        if len(table_info[1]) == 2:
            #print("TWoTimes")
            sql_command = "insert into pk_counts (dist) select count(1) from (select distinct " + table_info[1][0] + "," + table_info[1][1] + " from " + table_info[0] + " where (" + table_info[1][0]+"," +table_info[1][1]+") is not null) as primary_count"
            #print("SQL Command len == 2: ",sql_command)
            cur.execute(sql_command)
            refint.write(sql_command +'\n')
            conn.commit()
        if len(table_info[1]) == 3:
            #print("ThreeTimes")
            sql_command = "insert into pk_counts (dist) select count(1) from (select distinct " + table_info[1][0] + "," + \
                          table_info[1][1]+"," + table_info[1][2] + " from " + table_info[0] + " where (" + table_info[1][
                              0] + "," + table_info[1][1]+"," + table_info[1][2] + ") is not null) as primary_count"
            #print("SQL Command len == 3: ",sql_command)
            cur.execute(sql_command)
            refint.write(sql_command +'\n')
            conn.commit()
        # insert into pk_counts select count (k11), count (distinct k11)  , 'T1' from T1;

        # We'll add more to this line later. This is the answer table.
        #cur.execute("select min(dist) from pk_counts")
        #mintotal = cur.fetchall()
        #print(mintotal)
        #conn.commit()

        if len(table_info[2]) == 1: 
            sql_command = "insert into fk_counts (dist) select count(*) from "+ table_info[0]+ " where " + table_info[2][0] +" not in (select "+ table_info[2][0] +" from " + table_info[3][0] + ") or "+ table_info[2][0] +" is null"
            #print(sql_command)
            cur.execute(sql_command)
            refint.write(sql_command +'\n')
            conn.commit()
            #cur.execute("select min(dist) from fk_counts")
            #mintotal = cur.fetchall()
            #print(mintotal)
            conn.commit()
        if len(table_info[2]) == 2: 
            sql_command = "insert into fk_counts (dist) select count(*) from "+ table_info[0]+ " where " + table_info[2][0] +" not in (select "+ table_info[2][0] +" from " + table_info[3][0] + ") or "+ table_info[2][0] +" is null"
            #print(sql_command)
            cur.execute(sql_command)
            refint.write(sql_command +'\n')
            conn.commit()
            ql_command = "insert into fk_counts (dist) select count(*) from "+ table_info[0]+ " where " + table_info[2][1] +" not in (select "+ table_info[2][1] +" from " + table_info[3][0] + ") or "+ table_info[2][1] +" is null"
            #print(sql_command)
            cur.execute(sql_command)
            refint.write(sql_command +'\n')
            #conn.commit()
            #cur.execute("select min(dist) from fk_counts")
            #mintotal = cur.fetchall()
            #print(mintotal)
            conn.commit()


        if len(table_info[2]) == 0:
            sql_command = "insert into qm_table (tablename, entityerr, referentialerr, ok) values ('"+table_info[0]+"', (1.0 - (select min(dist)/ max(total) from pk_counts)), (0.0), 'N')"
            #print("SQL Command is: ", sql_command)
            cur.execute(sql_command)
            refint.write(sql_command +'\n')
            conn.commit()



        elif len(table_info[2]) == 1:
            sql_command = "insert into qm_table (tablename, entityerr, referentialerr, ok) values ('"+table_info[0]+"', (1.0 - (select min(dist)/ max(total) from pk_counts)), (select min(dist)/ max(total) from fk_counts), 'N')"
            #print("SQL Command is: ", sql_command)
            cur.execute(sql_command)
            refint.write(sql_command +'\n')
            conn.commit()
        else:
            sql_command = "insert into qm_table (tablename, entityerr, referentialerr, ok) values ('"+table_info[0]+"', (1.0 - (select min(dist)/ max(total) from pk_counts)), (select sum(dist)/ (max(total)*2) from fk_counts), 'N')"
            #print("SQL Command is: ", sql_command)
            cur.execute(sql_command)
            refint.write(sql_command +'\n')
            conn.commit()

     
        # insert into qm_table (entityerrpt ) values (1.0 - (select min (dist)/ max(total) from pk_counts  where name = 'T1'));

        #  (1.0 - (select min (dist)/ max(total) from pk_counts )
        sql_command = "update qm_table set ok = 'Y' where entityerr <= "+err+" and referentialerr <= "+err
        #cur.execute("update qm_table set ok = 'Y' where entityerr = 0.0 and referentialerr = 0.0")
        #refint.write("update qm_table set ok = 'Y' where entityerr = 0.0 and referentialerr = 0.0\n")
        cur.execute(sql_command)
        refint.write(sql_command +'\n')
        conn.commit()
        cur.execute("drop table pk_counts")
        refint.write("drop table pk_counts\n")
        conn.commit()
        cur.execute("drop table fk_counts")
        refint.write("drop table fk_counts\n")
        conn.commit()

        cur.close()


else:
    print("File failed to open!")

conn.close()

