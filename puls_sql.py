import mysql.connector
class puls_sql:
    def __init__(self, user, password, host, database):
        self.user = user
        self.password = password
        self.host = host
        self.database = database
        dateformat = '%Y-%m-%d %H:%M:%S'

    def readtable(self, query):
        rows = []
        cnx = mysql.connector.connect(user=self.user, password = self.password, host=self.host, database = self.database)
        cursor = cnx.cursor()
        cursor.execute(query)
        newrow = cursor.fetchone()
        while newrow is not None:
            try:
                newrow = list(newrow)
            except:
                newrow = newrow     #incase not iterable
            if len(rows) > 50000:
                return -1           #will not load bigger than 50k!!!
            rows.append(newrow)
            newrow = cursor.fetchone()
        return rows
                
    def put_table(self, command, data):
        try:
            cnx = mysql.connector.connect(user=self.user, password = self.password, host=self.host, database = self.database)
            cursor = cnx.cursor()
            cursor.execute(command, data)
            emp_no = cursor.lastrowid
            cnx.commit()
            cursor.close()
            cnx.close()
        except:
            return 'error'

    def get_headers(self, table):
        header = []
        query = ('describe ' + table)
        rows = self.readtable(query)
        for it in range(0, len(rows)):
            header.append(rows[it][0])
        return header

    def get_table(self, table):
        count = []
        headers = self.get_headers(table)
        for header in headers:
            try:
                modifier = header.split('_')[0]
                if modifier == 'pm':
                    count.append(header.split('_')[1])
                    headers.remove(header)
            except:
                modifier = False
        tempstr = ''
        for header in headers:
            tempstr += header + ','
        #print('select ' + tempstr.rstrip(',') + ' from ' + table)
        stuff = self.readtable('select ' + tempstr.rstrip(',') + ' from ' + table)
        matrix = [[0 for i in xrange(len(headers))] for i in xrange(len(stuff)+1)]
        for x in range(0, len(headers)):
            matrix[0][x] = str(headers[x])
        for y in range(0, len(stuff)):
            for x in range(0, len(headers)):
                matrix[y+1][x] = str(stuff[y][x])
        return matrix, count

    def get_pmcount(self, header, table):
        total = 0
        pmcolumnheader = 'pm_' + header
        pmcolumn = self.readtable('select ' + str(pmcolumnheader)+ ', id' + ' from ' + table)
        for iter1 in range(0, len(pmcolumn[:])):
            if pmcolumn[iter1][0] != None:
                lastpmid = pmcolumn[iter1][1]
        count = self.readtable('select '+header+' from ' + table + ' WHERE id!=' + str(lastpmid-1))
        for iter2 in range(0, len(count[:])):
            total += int(count[iter2][0])
        return total


    def get_databydate(self, sdate, edate, header, table):
        lastpmdate = lastpmdate.strftime(dateformat)
        lastpmdate = "'" + lastpmdate + "'"
        data = self.readtable('select '+header+' from ' + table + ' WHERE DATE(' + str(pmcolumn) + ') BETWEEN ' + lastpmdate + ' and now()')
        return data








