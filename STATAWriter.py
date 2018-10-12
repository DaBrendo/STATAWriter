import string

observlen = 2
column = ['1:3', '9:10', '13:13']
mainfile = open('STATA_Load_1mil.txt', 'w')
# merge 1:1 _n
for x in range(0, observlen):
    for y in column:
        templine1 = r'import delimited "\\xrdcwpappgme01\GME Research Data Repo\Division_Continental\Cornett_Brendon\Watts DVT Resubmit Labs FINAL.txt", encoding(utf8) colrange('

        inpoot1 = y

        templine2 = ') rowrange('

        if x == 0:
            inpoot2 = ''
        else:
            inpoot2 = str(1 + (x * 1000000))

        inpoot3 = str(1000000 + (x * 1000000))

        templine3 = ') stringcols(1 2 3)'

        templine4 = r'save "\\xrdcwpappgme01\GME Research Data Repo\Division_Continental\Cornett_Brendon\LAB_Split\Labs'

        inpoot4 = str(x + 1) + string.ascii_uppercase[column.index(y)]

        templine5 = r'.dta", replace'

        mergeline1 = templine1 + inpoot1 + templine2 + inpoot2 + templine3
        mergeline2 = templine4 + inpoot4 + templine5

        mainfile.write(mergeline1)
        mainfile.write('\r\n')
        mainfile.write(mergeline2)
        mainfile.write('\r\n')
        mainfile.write('clear')
        mainfile.write('\r\n')
        mainfile.write('\r\n')

    templine6 = 'using Labs'

    inpoot5 = str(x + 1) + 'A'

    templine7 = '.dta'

    mergeline3 = templine6 + inpoot5 + templine7

    mainfile.write(mergeline3 + '\r\n')
    k = 0
    for y in column:
        if k == 0:
            k += 1
        else:
            templine8 = 'merge 1:1 _n using Labs'

            inpoot6 = str(x + 1) + string.ascii_uppercase[(column.index(y))]

            templine9 = '.dta, assert(match) nogenerate'

            mergeline4 = templine8 + inpoot6 + templine9

            mainfile.write(mergeline4 + '\r\n')

    mainfile.write(r'save "\\xrdcwpappgme01\GME Research Data Repo\Division_Continental\Cornett_Brendon\LAB_Split\Labs' + str(x + 1) + r'.dta", replace' + '\r\n')
    mainfile.write('\r\n')
