# We import all the required modules at the beginning
''' In this project we deal with databases and data processing tools, hence we import sqlite3 which creates
    databases and helps in easy manipulation
'''
''' This project is used to analyse many csv files and extract data from them, we therfore use the tools present
    in the module csv
'''

# global initialization of the dictionaries and lists in which the extracted data from the csv files is stored.
# We use these data structures throughout the code, hence we globally
# initialize

'''
    to run on sqlite in terminal, type the following commands
    > sqlite3
    > .open record.db
    > SELECT * FROM Ticker;
    > SELECT * FROM Metrics;
'''
from mimetypes import init
import sqlite3
import csv
constraint = {}
# stores the conditions based on which confidence is alloted
prev_price = {}
# stores the prices of the stock of each of the companies on each of the
# consecutive days
companies = []
# stores the name of the companies as a list
initial_price = {}
# stores the starting price of the stock of a particular company
# used to store initial value for cases when a new company is added.
industries = []
# stores all the industries in a list


table = sqlite3.connect('record.db')
# This API opens a connection to the SQLite database file
cur = table.cursor()
# This routine creates a cursor which will be used throughout of your
# database programming with Python

# This routine executes an SQL statement
# Creates a Table named Ticker with 7 columns
cur.execute(""" CREATE TABLE Ticker(
    Date text,
    Company_Name text,
    Industry text,
    Previous_Day_Price text,
    Current_price text,
    Change_In_price text,
    Confidence text
)""")

# Creates a Table named Metrics with 2 columns
cur.execute(""" CREATE TABLE Metrics(
    KPIs text,
    Mertics text
)""")

# open Control Table file
with open(".\\Control\\control-table.csv", "r") as control:
    # start reading the control table csv file row by row
    control = csv.reader(control)
    # as the headings in the table forms the the first line, we exclude that
    # while reading using the next command
    next(control)
    rem_characters = "&<>=% "
    # remove special characters
    for row in control:
        # in each row, the line is divided into segments which are accessed by
        # the indexes from 0
        for character in rem_characters:
            row[1] = row[1].replace(character, "").strip()
            # replace the special characters in the file with an empty string
        industry = row[0]
        if industry in industries:
            re = 1
        elif (row[1] != "Previousdaynotlisted"):
            industries.append(industry)

        flag = 0
        # we input all the constraints into the constraints dictionary
        # we use this to compare the change% with the instructions given in the
        # control-table
        for key in constraint.keys():
            if (key == row[0]):
                flag = 1
        if (flag == 1 and row[1] != "Previousdaynotlisted"):
            constraint[row[0]].append(float(row[1]))
        elif (flag == 0):
            constraint[row[0]] = []
            if(row[1] != "Previousdaynotlisted"):
                constraint[row[0]].append(float(row[1]))

count = {}
# keeps a track of the count of the companies with the respective
# confidences ["Low","Medium","High"]
for industry in industries:
    count[industry] = [0] * 3

with open(".\\Record/2021113004-20-5-2022.csv", "r") as day_20:
    day_20 = csv.reader(day_20)
    next(day_20)
    for row in day_20:
        # adds the price in a prev_price storing dictionary
        prev_price[row[0]] = [float(row[2])]
        initial_price[row[0]] = [float(row[2])]
        # adds the new companies encountered into companies list
        companies.append(row[0])
        # insert the data from the first day into the ticker file
        # as previous day price is non existant -> we input NA for that column
        # as new companies are added -> we input 'Listed New'
        cur.execute("INSERT INTO Ticker VALUES (?,?,?,?,?,?,?)",
                    ('20-5-2022',
                     row[0],
                     row[1],
                     'NA',
                     row[2],
                     'NA',
                     'Listed New'))

idx = 0
with open(".\\Record/2021113004-21-5-2022.csv", "r") as day_21:
    day_21 = csv.reader(day_21)
    next(day_21)
    confidence = " "
    for row in day_21:
        # if the company is already existing in the market
        if row[0] in companies:
            prev_price[row[0]].append(float(row[2]))
            # var = change% of stock price wrt the previous day
            var = (((float(row[2]) - prev_price[row[0]]
                   [idx]) / prev_price[row[0]][idx]) * 100)
            industry = row[1]
            # compare with the constraints and assign confidence based on the
            # instructions given in the control-table
            if (var < constraint[industry][0]):
                count[industry][0] += 1
                confidence = "Low"
            elif (var > constraint[industry][2]):
                count[industry][2] += 1
                confidence = "High"
            else:
                count[industry][1] += 1
                confidence = "Medium"

        # if the company is newly listed
        else:
            # store the initial_price in a dictionary, as and when new companies are listed
            # as this will increase ease in calculation of gain% and loss%
            prev_price[row[0]] = [0, float(row[2])]
            initial_price[row[0]] = [float(row[2])]
            confidence = 'Listed New'
            companies.append(row[0])

        # as prev_day price exists, we have a confidenc to input now
        cur.execute("INSERT INTO Ticker VALUES (?,?,?,?,?,?,?)",
                    ('21-5-2022',
                     row[0],
                     row[1],
                     prev_price[row[0]][idx],
                     row[2],
                     var,
                     confidence))


idx += 1
# all the processes as shown in day_21 is repeated for the remaining files
with open(".\\Record/2021113004-22-5-2022.csv", "r") as day_22:
    day_22 = csv.reader(day_22)
    next(day_22)
    confidence = " "
    for row in day_22:
        if row[0] in companies:
            prev_price[row[0]].append(float(row[2]))
            var = (((float(row[2]) - prev_price[row[0]]
                   [idx]) / prev_price[row[0]][idx]) * 100)
            industry = row[1]
            if (var < constraint[industry][0]):
                count[industry][0] += 1
                confidence = "Low"
            elif (var > constraint[industry][2]):
                count[industry][2] += 1
                confidence = "High"
            else:
                count[industry][1] += 1
                confidence = "Medium"
        else:
            prev_price[row[0]] = [0, 0, float(row[2])]
            initial_price[row[0]] = [float(row[2])]
            confidence = 'Listed New'
            companies.append(row[0])

        cur.execute("INSERT INTO Ticker VALUES (?,?,?,?,?,?,?)",
                    ('22-5-2022',
                     row[0],
                     row[1],
                     prev_price[row[0]][idx],
                     row[2],
                     var,
                     confidence))

idx += 1
# all the processes as shown in day_21 is repeated for the remaining files
with open(".\\Record/2021113004-23-5-2022.csv", "r") as day_23:
    day_23 = csv.reader(day_23)
    next(day_23)
    confidence = " "
    for row in day_23:
        if row[0] in companies:
            prev_price[row[0]].append(float(row[2]))
            var = (((float(row[2]) - prev_price[row[0]]
                   [idx]) / prev_price[row[0]][idx]) * 100)
            industry = row[1]
            if (var < constraint[industry][0]):
                count[industry][0] += 1
                confidence = "Low"
            elif (var > constraint[industry][2]):
                count[industry][2] += 1
                confidence = "High"
            else:
                count[industry][1] += 1
                confidence = "Medium"
        else:
            prev_price[row[0]] = [0, 0, 0, float(row[2])]
            initial_price[row[0]] = [float(row[2])]
            confidence = 'Listed New'
            companies.append(row[0])

        cur.execute("INSERT INTO Ticker VALUES (?,?,?,?,?,?,?)",
                    ('23-5-2022',
                     row[0],
                     row[1],
                     prev_price[row[0]][idx],
                     row[2],
                     var,
                     confidence))

idx += 1
# all the processes as shown in day_21 is repeated for the remaining files
with open(".\\Record/2021113004-24-5-2022.csv", "r") as day_24:
    day_24 = csv.reader(day_24)
    next(day_24)
    confidence = " "
    for row in day_24:
        if row[0] in companies:
            prev_price[row[0]].append(float(row[2]))
            var = (((float(row[2]) - prev_price[row[0]]
                   [idx]) / prev_price[row[0]][idx]) * 100)
            industry = row[1]
            if (var < constraint[industry][0]):
                count[industry][0] += 1
                confidence = "Low"
            elif (var > constraint[industry][2]):
                count[industry][2] += 1
                confidence = "High"
            else:
                count[industry][1] += 1
                confidence = "Medium"
        else:
            prev_price[row[0]] = [0, 0, 0, 0, float(row[2])]
            confidence = 'Listed New'
            initial_price[row[0]] = [float(row[2])]
            companies.append(row[0])

        cur.execute("INSERT INTO Ticker VALUES (?,?,?,?,?,?,?)",
                    ('24-5-2022',
                     row[0],
                     row[1],
                     prev_price[row[0]][idx],
                     row[2],
                     var,
                     confidence))

# find highest listing company
# we store the count of the number of highs, lows and medium from count
# dictionary in variables and find their max
max_count_high = 0
for industry in industries:
    if (max_count_high < count[industry][2]):
        max_count_high = count[industry][2]
        largest = industry

# gain%
# we have 2 parameters for comparison for the companies:-
    # -> compare the gain%
    # -> if multiple companies have the same gain%, the company returnes as the best company = company with max gain
gain_dict = {}
diff = {}
# diff dictionary contains the list of the companies with the same gain%
for comp in companies:
    gain_dict[comp] = ((prev_price[comp][4] - initial_price[comp]
                       [0]) / float(initial_price[comp][0])) * 100
max_gain = -100
for comp in companies:
    if(max_gain < gain_dict[comp]):
        max_gain = gain_dict[comp]
        diff[max_gain] = [comp]
    if(max_gain == gain_dict[comp]):
        max_gain = gain_dict[comp]
        diff[max_gain].append(comp)

diff_dict = {}
# stores the difference in the final and initial stock price
for comp in diff[max_gain]:
    diff_dict[comp] = (prev_price[comp][4] - initial_price[comp][0])
# sorting the dictionary based on items in the dictionary
sorted_diff_dict = {
    k: v for k,
    v in sorted(
        diff_dict.items(),
        key=lambda item: item[1])}

for key in diff_dict.keys():
    best_comp = key

# loss%
# we have 2 parameters for comparison for the companies:-
    # -> compare the loss%
    # -> if multiple companies have the same loss%, the company returnes as the worst company = company with max loss
loss_dict = {}
diff = {}
for comp in companies:
    loss_dict[comp] = -(((prev_price[comp][4]) -
                        initial_price[comp][0]) / initial_price[comp][0]) * 100
max_loss = -100
for comp in companies:
    if (max_loss < loss_dict[comp]):
        max_loss = loss_dict[comp]
        diff[max_loss] = [comp]
    if(max_loss == gain_dict[comp]):
        max_loss = gain_dict[comp]
        diff[max_loss].append(comp)

diff_dict = {}
for comp in diff[max_loss]:
    diff_dict[comp] = -(prev_price[comp][4] - initial_price[comp][0])
sorted_diff_dict = {
    k: v for k,
    v in sorted(
        diff_dict.items(),
        key=lambda item: item[1])}


for key in diff_dict.keys():
    worst_comp = key
    break

# find lowest listing company
max_count_low = 0
for industry in industries:
    if(max_count_low < count[industry][0]):
        max_count_low = count[industry][0]
        smallest = industry

# execute insert statements into the database Table Metrics
cur.execute("INSERT INTO Metrics VALUES (?,?)",
            ('Best Listed Industry',
             largest))
cur.execute("INSERT INTO Metrics VALUES (?,?)",
            ('Best Company',
             best_comp))
cur.execute("INSERT INTO Metrics VALUES (?,?)",
            ('Gain%',
             max_gain))
cur.execute("INSERT INTO Metrics VALUES (?,?)",
            ('Worst Listed Industry',
             smallest))
cur.execute("INSERT INTO Metrics VALUES (?,?)",
            ('Worst Company',
             worst_comp))
cur.execute("INSERT INTO Metrics VALUES (?,?)",
            ('Loss%',
             -max_loss))

# print the table onto the terminal using the below commands
cur.execute("SELECT * FROM Ticker")
# executes the sql statement
items = cur.fetchall()
# items as a list fetches all the output given by the sql database and
# stores it
for item in items:
    print(item)

cur.execute("SELECT * FROM Metrics")
items = cur.fetchall()
for item in items:
    print(item)

table.commit()
# commmit the database changes into the database files
cur.close()
# close the cursor which execute sql commands
