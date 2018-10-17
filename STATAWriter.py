import string
import os
import math

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


def labs(argfile, argobslen, argsplitlen, argcolumn):
    looplen = int(math.ceil(argobslen / argsplitlen))
    if os.path.exists(argfile):
        os.remove(argfile)
    outpit = open(argfile, 'w')
    for x in range(0, looplen):
        for y in argcolumn:
            templine1 = r'import delimited ".\Labs.txt", encoding(utf8) varnames(1) case(upper) colrange('

            templine2 = ') rowrange('
            if x == 0:
                inpoot1 = ''
                inpoot2 = str(((x + 1) * argsplitlen))
            else:
                inpoot1 = str(x + (x * argsplitlen))
                inpoot2 = str(x + ((x + 1) * argsplitlen))

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

        outpit.write('use .\LAB_Split\Labs' + str(x + 1) + 'A' + '.dta' + '\r\n')
        for y in argcolumn:
            if argcolumn.index(y) > 0:
                outpit.write('merge 1:1 _n using .\Lab_Split\Labs' + str(x + 1) + string.ascii_uppercase[
                    argcolumn.index(y)] + '.dta, nogenerate')
                outpit.write('\r\n')

        outpit.write('gen UUID = HOSPID + PTID' + '\r\n')
        outpit.write('drop HOSPID' + '\r\n')
        outpit.write('drop PTID' + '\r\n')
        outpit.write('order UUID' + '\r\n')
        outpit.write('destring RESULT_ANSWER_TEXT, generate(RESULTS_NUM) force' + '\r\n')
        outpit.write('generate str RESULTS_STR = RESULT_ANSWER_TEXT if RESULTS_NUM ==.' + '\r\n')
        outpit.write('drop RESULT_ANSWER_TEXT' + '\r\n')
        outpit.write('order RESULTS_STR, after(RESULTS_NUM)' + '\r\n')
        outpit.write('replace LOINC_CODE = "" if LOINC_CODE == "?"' + '\r\n')
        outpit.write('rename PROC_NAME PROCEDURE' + '\r\n')
        outpit.write(r'save ".\LAB_Split\Joined\Labs' + str(x + 1) + r'.dta", replace' + '\r\n')
        outpit.write('clear')
        outpit.write('\r\n')
        outpit.write('\r\n')


def bmi(argfile):
    if os.path.exists(argfile):
        os.remove(argfile)
    outpit = open(argfile, 'w')
    outpit.write(r'import delimited ".\BMI.txt", varnames(1) case(upper) encoding(utf8) stringcols(_all)' + '\r\n')
    outpit.write('gen UUID = HOSPID + PTID' + '\r\n')
    outpit.write('order UUID' + '\r\n')
    outpit.write('drop HOSPID' + '\r\n')
    outpit.write('drop PTID' + '\r\n')
    outpit.write('rename HEIGHT_CENTIMETERS HEIGHT' + '\r\n')
    outpit.write('rename WEIGHT_KILOGRAMS WEIGHT' + '\r\n')
    outpit.write('rename CALCULATED_BMI BMI' + '\r\n')
    outpit.write('destring HEIGHT, replace' + '\r\n')
    outpit.write('destring WEIGHT, replace' + '\r\n')
    outpit.write('destring BMI, replace' + '\r\n')
    outpit.write(r'save ".\Imported\BMI.dta", replace')


def demographic(argfile):
    if os.path.exists(argfile):
        os.remove(argfile)
    outpit = open(argfile, 'w')
    outpit.write(
        r'import delimited ".\Demographics.txt", varnames(1) case(upper) encoding(utf8) stringcols(_all)' + '\r\n')
    outpit.write(r'gen UUID = HOSPID + PTID' + '\r\n')
    outpit.write(r'drop HOSPID' + '\r\n')
    outpit.write(r'drop PTID' + '\r\n')
    outpit.write(r'order UUID' + '\r\n')
    outpit.write(r'drop ETHNICITY' + '\r\n')
    outpit.write(r'replace RACE1 = upper(RACE1)' + '\r\n')
    outpit.write(r'replace RACE2 = upper(RACE2)' + '\r\n')
    outpit.write(r'gen str RACE1FIX = ""' + '\r\n')
    outpit.write(r'gen str RACE2FIX = ""' + '\r\n')
    outpit.write(r'gen str RACE = ""' + '\r\n')
    outpit.write(
        r'replace RACE1FIX = "ASIAN" if RACE1 == "A" | RACE1 == "AS" | RACE1 == "ASIAN" | RACE1 == "OR" | RACE1 == "ASIA"' + '\r\n')
    outpit.write(
        r'replace RACE1FIX = "AMIN" if RACE1 == "AI" | RACE1 == "INDI" | RACE1 == "AMIN" | RACE1 == "I"' + '\r\n')
    outpit.write(
        r'replace RACE1FIX = "BLACK" if RACE1 == "B" | RACE1 == "AA" | RACE1 == "AF" | RACE1 == "AFRO" | RACE1 == "BL" | RACE1 == "BLAC" | RACE1 == "BLACK"' + '\r\n')
    outpit.write(r'replace RACE1FIX = "HAWAII" if RACE1 == "HAWAIIAN"' + '\r\n')
    outpit.write(
        r'replace RACE1FIX = "OTHER" if RACE1 == "N" | RACE1 == "O" | RACE1 == "OT" | RACE1 == "OTH" | RACE1 == "OTHER"' + '\r\n')
    outpit.write(
        r'replace RACE1FIX = "HISPANIC" if RACE1 == "H" | RACE1 == "HI" | RACE1 == "HIS" | RACE1 == "HISP" | RACE1 == "LA" | RACE1 == "HW" | RACE1 == "HB"' + '\r\n')
    outpit.write(
        r'replace RACE1FIX = "OTHER" if RACE1 == "U" | RACE1 == "UN" | RACE1 == "UNK" | RACE1 == "UNKN"' + '\r\n')
    outpit.write(
        r'replace RACE1FIX = "WHITE" if RACE1 == "WHITE" | RACE1 == "C" | RACE1 == "CA" | RACE1 == "CAUC" | RACE1 == "W" | RACE1 == "WH" | RACE1 == "WHI"' + '\r\n')
    outpit.write(
        r'replace RACE1FIX = "MULTI" if RACE1 == "M" | RACE1 == "MR" | RACE1 == "MU" | RACE1 == "MUL"| RACE1 == "MULT"' + '\r\n')
    outpit.write(r'drop RACE1' + '\r\n')
    outpit.write(
        r'replace RACE2FIX = "ASIAN" if RACE2 == "A" | RACE1 == "AS" | RACE1 == "ASIAN" | RACE1 == "OR" | RACE1 == "ASIA"' + '\r\n')
    outpit.write(
        r'replace RACE2FIX = "AMIN" if RACE2 == "AI" | RACE1 == "INDI" | RACE1 == "AMIN" | RACE1 == "I"' + '\r\n')
    outpit.write(
        r'replace RACE2FIX = "BLACK" if RACE2 == "B" | RACE1 == "AA" | RACE1 == "AF" | RACE1 == "AFRO" | RACE1 == "BL" | RACE1 == "BLAC" | RACE1 == "BLACK"' + '\r\n')
    outpit.write(r'replace RACE2FIX = "HAWAII" if RACE2 == "HAWAIIAN"' + '\r\n')
    outpit.write(
        r'replace RACE2FIX = "OTHER" if RACE2 == "N" | RACE1 == "O" | RACE1 == "OT" | RACE1 == "OTH" | RACE1 == "OTHER"' + '\r\n')
    outpit.write(
        r'replace RACE2FIX = "HISPANIC" if RACE2 == "H" | RACE1 == "HI" | RACE1 == "HIS" | RACE1 == "HISP" | RACE1 == "LA" | RACE1 == "HW" | RACE1 == "HB"' + '\r\n')
    outpit.write(
        r'replace RACE2FIX = "OTHER" if RACE2 == "U" | RACE1 == "UN" | RACE1 == "UNK" | RACE1 == "UNKN"' + '\r\n')
    outpit.write(
        r'replace RACE2FIX = "WHITE" if RACE2 == "WHITE" | RACE1 == "C" | RACE1 == "CA" | RACE1 == "CAUC" | RACE1 == "W" | RACE1 == "WH" | RACE1 == "WHI"' + '\r\n')
    outpit.write(
        r'replace RACE2FIX = "MULTI" if RACE2 == "M" | RACE1 == "MR" | RACE1 == "MU" | RACE1 == "MUL"| RACE1 == "MULT"' + '\r\n')
    outpit.write(r'drop RACE2' + '\r\n')
    outpit.write(r'replace RACE = "ASIAN" if RACE1FIX == "ASIAN" & RACE2FIX == "ASIAN"' + '\r\n')
    outpit.write(r'replace RACE = "AMIN" if RACE1FIX == "AMIN" & RACE2FIX == "AMIN"' + '\r\n')
    outpit.write(r'replace RACE = "BLACK" if RACE1FIX == "BLACK" & RACE2FIX == "BLACK"' + '\r\n')
    outpit.write(r'replace RACE = "HAWAII" if RACE1FIX == "HAWAII" & RACE2FIX == "HAWAII"' + '\r\n')
    outpit.write(r'replace RACE = "OTHER" if RACE1FIX == "OTHER" & RACE2FIX == "OTHER"' + '\r\n')
    outpit.write(r'replace RACE = "HISPANIC" if RACE1FIX == "HISPANIC" & RACE2FIX == "HISPANIC"' + '\r\n')
    outpit.write(r'replace RACE = "WHITE" if RACE1FIX == "WHITE" & RACE2FIX == "WHITE"' + '\r\n')
    outpit.write(r'replace RACE = "MULTI" if RACE1FIX == "MULTI" & RACE2FIX == "MULTI"' + '\r\n')
    outpit.write(r'replace RACE = "MULTI" if RACE == ""' + '\r\n')
    outpit.write(r'drop RACE1FIX' + '\r\n')
    outpit.write(r'drop RACE2FIX' + '\r\n')
    outpit.write('save ".\Imported\Demographics.dta", replace')
    outpit.write('\r\n')


observlen = 18000000
column = ['1:3', '9:10', '14:14']

labs('LabLoad.do', observlen, 5000000, column)

bmi('BMILoad.do')

demographic('DemographicLoad.do')
