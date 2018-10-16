import string
import os
import math


def labs(argfile, argobslen, argsplitlen, argcolumn):
    looplen = int(math.ceil(argobslen / argsplitlen))
    if os.path.exists(argfile):
        os.remove(argfile)
    outpit = open(argfile, 'w')
    for x in range(0, looplen):
        for y in argcolumn:
            templine1 = r'import delimited ".\Labs.txt", encoding(utf8) varnames(1) case(upper) colrange('

            templine2 = ') rowrange('

            inpoot1 = str(x * argsplitlen)
            inpoot2 = str(1 + ((x + 1) * argsplitlen))

            templine3 = ') stringcols(_all)'

            templine4 = r'save ".\LAB_Split\Labs'

            inpoot4 = str(x + 1) + string.ascii_uppercase[argcolumn.index(y)]

            templine5 = r'.dta", replace'

            mergeline1 = templine1 + y + templine2 + inpoot1 + ':' + inpoot2 + templine3
            mergeline2 = templine4 + inpoot4 + templine5

            outpit.write(mergeline1)
            outpit.write('\r\n')
            outpit.write(mergeline2)
            outpit.write('\r\n')
            outpit.write('clear')
            outpit.write('\r\n')
            outpit.write('\r\n')

        outpit.write('use .\LAB_Split\Labs' + str(x + 1) + 'A' + '.dta, nogenerate' + '\r\n')
        for y in argcolumn:
            if argcolumn.index(y) > 0:
                outpit.write('merge 1:1 _n using Labs' + str(x + 1) + string.ascii_uppercase[argcolumn.index(y)] + '.dta')
                outpit.write('\r\n')

        outpit.write('gen UUID = HOSPID + PTID' + '\r\n')
        outpit.write('drop HOSPID' + '\r\n')
        outpit.write('drop PTID' + '\r\n')
        outpit.write('order UUID' + '\r\n')
        outpit.write(r'save ".\LAB_Split\Joined\Labs' + str(x + 1) + r'.dta", replace' + '\r\n')
        outpit.write('clear')
        outpit.write('\r\n')
        outpit.write('\r\n')


observlen = 18000000
column = ['1:3', '9:10', '13:13']


labs('STATA_Load.txt', observlen, 1000000, column)
