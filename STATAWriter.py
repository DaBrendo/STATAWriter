import string
import os

# def blocks(files, size=65536):
#    while True:
#        b = files.read(size)
#        if not b:
#            break
#        yield b
#
#
# with open("file", "r", encoding="utf-8", errors='ignore') as f:
#    print(sum(bl.count(r'\n') for bl in blocks(f)))


def labread(loadfile, observationlength, splitlength, splitcolumn):
    iterater = int(observationlength / splitlength)

    if os.path.exists((loadfile + '.txt')):
        os.remove((loadfile + '.txt'))

    labout = open((loadfile + '.txt'), 'w')

    for x in range(0, iterater):
        for y in splitcolumn:
            templine1 = r'import delimited "Labs.txt", encoding(utf8) varnames(1) case(upper) colrange('

            inpoot1 = y

            templine2 = ') rowrange('

            if x == 0:
                inpoot2 = ''
            else:
                inpoot2 = str(1 + (x * splitlength))

            inpoot3 = str(splitlength + (x * splitlength))

            templine3 = ') stringcols(_all)'

            templine4 = r'save ".\LAB_Split\Labs'

            inpoot4 = str(x + 1) + string.ascii_uppercase[splitcolumn.index(y)]

            templine5 = r'.dta", replace'

            mergeline1 = templine1 + inpoot1 + templine2 + inpoot2 + ':' + inpoot3 + templine3
            mergeline2 = templine4 + inpoot4 + templine5

            labout.write(mergeline1)
            labout.write('\r\n')
            labout.write(mergeline2)
            labout.write('\r\n')
            labout.write('clear')
            labout.write('\r\n')
            labout.write('\r\n')

        templine6 = 'use .\LAB_Split\Labs'

        inpoot5 = str(x + 1) + 'A'

        templine7 = '.dta'

        mergeline3 = templine6 + inpoot5 + templine7

        labout.write(mergeline3 + '\r\n')

        k = 0

        for y in splitcolumn:
            if k == 0:
                k += 1
            else:
                templine8 = 'merge 1:1 _n using .\LAB_Split\Labs'

                inpoot6 = str(x + 1) + string.ascii_uppercase[(splitcolumn.index(y))]

                templine9 = '.dta, nogenerate'

                mergeline4 = templine8 + inpoot6 + templine9

                labout.write(mergeline4 + '\r\n')

        labout.write('gen UUID = HOSPID + PTID' + '\r\n')
        labout.write('drop HOSPID' + '\r\n')
        labout.write('drop PTID' + '\r\n')
        labout.write('order UUID' + '\r\n')

        labout.write(r'save ".\LAB_Split\Joined\Labs' + str(x + 1) + r'.dta", replace' + '\r\n')
        labout.write('clear')
        labout.write('\r\n')
        labout.write('\r\n')


observlen = 120000000
splitby = 5000000
column = ['1:3', '9:10', '14:14']

mainfile = 'STATA_Load'

labread(mainfile, observlen, splitby, column)
