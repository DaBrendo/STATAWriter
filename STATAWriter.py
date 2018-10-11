observlen = 120
column = '1:3' '9:10' '13:13'
mainfile = open('STATA_Load_1mil.txt', 'w')
# merge 1:1 _n
for x in range(0, observlen):
    for y in column:
        templine1 = r'import delimited "\\xrdcwpappgme01\GME Research Data Repo\Division_Continental\Cornett_Brendon\Watts DVT Resubmit Labs FINAL.txt", encoding(utf8) colrange(:3) rowrange('

        if x == 0:
            inpoot1 = ''
        else:
            inpoot1 = str(1 + (x * 1000000))

        inpoot2 = str(1000000 + (x * 1000000))

        templine2 = ') stringcols(1 2 3)'

        templine3 = r'save "\\xrdcwpappgme01\GME Research Data Repo\Division_Continental\Cornett_Brendon\LAB_Split\Labs'

        inpoot3 = str(x + 1) + 'A'

        templine4 = r'.dta", replace'

        mergeline1 = templine1 + inpoot1 + ':' + inpoot2 + templine2
        mergeline2 = templine3 + inpoot3 + templine4

        mainfile.write(mergeline1 + '\r\n')
        mainfile.write('\r\n')
        mainfile.write(mergeline2 + '\r\n')
        mainfile.write('\r\n')
        mainfile.write('clear' + '\r\n')
        mainfile.write('\r\n')
        mainfile.write('\r\n')

        templine1 = r'import delimited "\\xrdcwpappgme01\GME Research Data Repo\Division_Continental\Cornett_Brendon\Watts DVT Resubmit Labs FINAL.txt", encoding(utf8) colrange(9:10) rowrange('

    if x == 0:
        inpoot1 = ''
    else:
        inpoot1 = str(1 + (x * 1000000))

    inpoot2 = str(1000000 + (x * 1000000))

    templine2 = ') stringcols(1 2 3)'

    templine3 = r'save "\\xrdcwpappgme01\GME Research Data Repo\Division_Continental\Cornett_Brendon\LAB_Split\Labs'

    inpoot3 = str(x + 1) + 'B'

    templine4 = r'.dta", replace'

    mergeline1 = templine1 + inpoot1 + ':' + inpoot2 + templine2
    mergeline2 = templine3 + inpoot3 + templine4

    mainfile.write(mergeline1 + '\r\n')
    mainfile.write('\r\n')
    mainfile.write(mergeline2 + '\r\n')
    mainfile.write('\r\n')
    mainfile.write('clear' + '\r\n')
    mainfile.write('\r\n')
    mainfile.write('\r\n')

    templine1 = r'import delimited "\\xrdcwpappgme01\GME Research Data Repo\Division_Continental\Cornett_Brendon\Watts DVT Resubmit Labs FINAL.txt", encoding(utf8) colrange(13:13) rowrange('

    if x == 0:
        inpoot1 = ''
    else:
        inpoot1 = str(1 + (x * 1000000))

    inpoot2 = str(1000000 + (x * 1000000))

    templine2 = ') stringcols(1 2 3)'

    templine3 = r'save "\\xrdcwpappgme01\GME Research Data Repo\Division_Continental\Cornett_Brendon\LAB_Split\Labs'

    inpoot3 = str(x + 1) + 'C'

    templine4 = r'.dta", replace'

    mergeline1 = templine1 + inpoot1 + ':' + inpoot2 + templine2
    mergeline2 = templine3 + inpoot3 + templine4

    mainfile.write(mergeline1 + '\r\n')
    mainfile.write('\r\n')
    mainfile.write(mergeline2 + '\r\n')
    mainfile.write('\r\n')
    mainfile.write('clear' + '\r\n')
    mainfile.write('\r\n')
    mainfile.write('\r\n')
