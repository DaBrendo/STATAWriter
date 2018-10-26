import string
import os
import math
import csv
import pathlib

# def is_valid_directory(filename):
#    p = pathlib.Path(filename)
#    return p.exists() and p.is_dir()
#
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
            if x == 0:
                outpit.write(
                    r'import delimited ".\Labs.txt", encoding(utf8) varnames(1) case(upper) colrange(' + y + ') rowrange(' + '' + ':' + str(((x + 1) * argsplitlen)) + ') stringcols(_all)' + '\r\n')
            else:
                outpit.write(
                    r'import delimited ".\Labs.txt", encoding(utf8) varnames(1) case(upper) colrange(' + y + ') rowrange(' + str(x + (x * argsplitlen)) + ':' + str(x + ((x + 1) * argsplitlen)) + ') stringcols(_all)' + '\r\n')
            outpit.write(r'save ".\LAB_Split\Labs' + str(x + 1) + string.ascii_uppercase[argcolumn.index(y)] + r'.dta", replace' + '\r\n')
            outpit.write('clear')
            outpit.write('\r\n')
        outpit.write('use .\LAB_Split\Labs' + str(x + 1) + 'A' + '.dta' + '\r\n')
        for y in argcolumn:
            if argcolumn.index(y) > 0:
                outpit.write('merge 1:1 _n using .\Lab_Split\Labs' + str(x + 1) + string.ascii_uppercase[
                    argcolumn.index(y)] + '.dta, nogenerate')
                outpit.write('\r\n')
        outpit.write(r'recode * ("?"="")' + '\r\n')
        outpit.write('destring RESULT_ANSWER_TEXT, generate(RESULTS_NUM) force' + '\r\n')
        outpit.write('generate str RESULTS_STR = RESULT_ANSWER_TEXT if RESULTS_NUM ==.' + '\r\n')
        outpit.write('drop RESULT_ANSWER_TEXT' + '\r\n')
        outpit.write('order RESULTS_STR, after(RESULTS_NUM)' + '\r\n')
        outpit.write('replace LOINC_CODE = "" if LOINC_CODE == "?"' + '\r\n')
        outpit.write('rename PROC_NAME PROCEDURE' + '\r\n')
        outpit.write(r'save ".\LAB_Split\Joined\Labs' + str(x + 1) + r'.dta", replace' + '\r\n')
        outpit.write('clear' + '\r\n')


def bmi(argfile):
    if os.path.exists(argfile):
        os.remove(argfile)
    outpit = open(argfile, 'w')
    outpit.write(r'import delimited ".\BMI.txt", varnames(1) case(upper) encoding(utf8) stringcols(_all)' + '\r\n')
    outpit.write(r'recode * ("?"="")' + '\r\n')
    outpit.write('rename HEIGHT_CENTIMETERS HEIGHT' + '\r\n')
    outpit.write('rename WEIGHT_KILOGRAMS WEIGHT' + '\r\n')
    outpit.write('rename CALCULATED_BMI BMI' + '\r\n')
    outpit.write('destring WEIGHT, replace' + '\r\n')
    outpit.write('destring BMI, replace' + '\r\n')
    outpit.write(r'save ".\Imported\BMI.dta", replace' + '\r\n')
    outpit.write('clear' + '\r\n')


def demographic(argfile):
    if os.path.exists(argfile):
            os.remove(argfile)
    outpit = open(argfile, 'w')
    outpit.write(
        r'import delimited ".\Demographics.txt", varnames(1) case(upper) encoding(utf8) stringcols(_all)' + '\r\n')
    outpit.write(r'rename ESRI_GROUP ESRI' + '\r\n')
    outpit.write(r'drop ETHNICITY' + '\r\n')
    outpit.write(r'recode * ("?"="")' + '\r\n')
    outpit.write(r'replace RACE1 = upper(RACE1)' + '\r\n')
    outpit.write(r'replace RACE2 = upper(RACE2)' + '\r\n')
    outpit.write(r'gen str RACE1FIX = ""' + '\r\n')
    outpit.write(r'gen str RACE2FIX = ""' + '\r\n')
    outpit.write(r'gen str RACE = ""' + '\r\n')
    for file in os.listdir((os.curdir + r'/Race')):
        filename = os.fsdecode(file)
        if filename.endswith(".csv"):
            with open((os.curdir + r'/Race' + r'/' + filename), 'r') as csv_file:
                readin = list(csv.reader(csv_file, delimiter=','))
            racelist = [item for sublist in readin for item in sublist]
            racelist = [x.upper() for x in racelist]
            for x in racelist:
                if racelist.index(x) > 0:
                    outpit.write(r'replace RACE1FIX = "' + racelist[0] + r'" ' + 'if RACE1 == "' + x + r'"' + '\r\n')
                    outpit.write(r'replace RACE2FIX = "' + racelist[0] + r'" ' + 'if RACE2 == "' + x + r'"' + '\r\n')
            outpit.write(r'replace RACE = "' + racelist[0] + r'" ' + 'if RACE1FIX == "' + racelist[0] + r'" & RACE2FIX == "' + racelist[0] + '"' + '\r\n')
            outpit.write(r'replace RACE = "' + racelist[0] + r'" ' + 'if RACE1 == "' + r'?' + r'" & RACE2FIX == "' + racelist[0] + '"' + '\r\n')
            outpit.write(r'replace RACE = "' + racelist[0] + r'" ' + 'if RACE1FIX == "' + racelist[0] + r'" & RACE2 == "' + r'?' + '"' + '\r\n')
            outpit.write(r'replace RACE = "' + racelist[0] + r'" ' + 'if RACE1 == "' + r'" & RACE2FIX == "' + racelist[0] + '"' + '\r\n')
            outpit.write(r'replace RACE = "' + racelist[0] + r'" ' + 'if RACE1FIX == "' + racelist[0] + r'" & RACE2 == "' + '"' + '\r\n')
    outpit.write(r'replace RACE = "MULTIRACIAL" if RACE == ""' + '\r\n')
    outpit.write(r'drop RACE1FIX' + '\r\n')
    outpit.write(r'drop RACE2FIX' + '\r\n')
    outpit.write(r'drop RACE1' + '\r\n')
    outpit.write(r'drop RACE2' + '\r\n')
    outpit.write(r'drop if ESRI == ""' + '\r\n')
    outpit.write('save ".\Imported\Demographics.dta", replace' + '\r\n')
    outpit.write('clear' + '\r\n')


def diagnosis(argfile):
    if os.path.exists(argfile):
        os.remove(argfile)
    outpit = open(argfile, 'w')
    outpit.write(
        r'import delimited ".\Diagnoses.txt", varnames(1) case(upper) encoding(utf8) stringcols(_all)' + '\r\n')
    outpit.write(r'drop DX DX_CODETYPE ORIGDX' + '\r\n')
    outpit.write(r'replace DXDESC = upper(DXDESC)' + '\r\n')
    outpit.write(r'gen DIAGCAT = ""' + '\r\n')
    for file in os.listdir((os.curdir + r'/Diagnoses')):
        filename = os.fsdecode(file)
        if filename.endswith(".csv"):
            with open((os.curdir + r'/Diagnoses' + r'/' + filename), 'r') as csv_file:
                readin = list(csv.reader(csv_file, delimiter=','))
            diaglist = [item for sublist in readin for item in sublist]
            diaglist = [x.upper() for x in diaglist]
            for x in diaglist:
                outpit.write(r'replace DIAGCAT = "' + diaglist[0] + r'" ' + 'if DXDESC == "' + x + r'"' + '\r\n')
    outpit.write(r'drop if DIAGCAT == ""' + '\r\n')
    outpit.write('save ".\Imported\Diagnoses.dta", replace' + '\r\n')
    outpit.write('clear' + '\r\n')


def pharmacy(argfile):
    if os.path.exists(argfile):
        os.remove(argfile)
    outpit = open(argfile, 'w')
    outpit.write(
        r'import delimited ".\Pharmacy.txt", varnames(1) case(upper) encoding(utf8) stringcols(_all)' + '\r\n')
    outpit.write(r'drop ADMIN_TIME PHRMMNEM GENERICPHRMMNEM THERCLSGRP GENNAMEGRP SPECNAMEGRP RX_NDC' + '\r\n')
    outpit.write(r'replace MED = upper(MED)' + '\r\n')
    outpit.write(r'gen MEDCAT = ""' + '\r\n')
    for file in os.listdir((os.curdir + r'/Pharma')):
        filename = os.fsdecode(file)
        if filename.endswith(".csv"):
            with open((os.curdir + r'/Pharma' + r'/' + filename), 'r') as csv_file:
                readin = list(csv.reader(csv_file, delimiter=','))
            medlist = [item for sublist in readin for item in sublist]
            medlist = [x.upper() for x in medlist]
            for x in medlist:
                outpit.write(r'replace MEDCAT = "' + medlist[0] + r'" ' + 'if MED == "' + x + r'"' + '\r\n')
    outpit.write(r'drop if MEDCAT == ""' + '\r\n')
    outpit.write('save ".\Imported\Pharmacy.dta", replace' + '\r\n')
    outpit.write('clear' + '\r\n')


def procedure(argfile):
    if os.path.exists(argfile):
        os.remove(argfile)
    outpit = open(argfile, 'w')
    outpit.write(
        r'import delimited ".\Procedures.txt", varnames(1) case(upper) encoding(utf8) stringcols(_all)' + '\r\n')

def readmit(argfile):
    if os.path.exists(argfile):
        os.remove(argfile)
    outpit = open(argfile, 'w')
    outpit.write(
        r'import delimited ".\Readmit.txt", varnames(1) case(upper) encoding(utf8) stringcols(_all)' + '\r\n')


def encounter(argfile):
    if os.path.exists(argfile):
        os.remove(argfile)
    outpit = open(argfile, 'w')
    outpit.write(
        r'import delimited ".\Encounters.txt", varnames(1) case(upper) encoding(utf8) stringcols(_all)' + '\r\n')


def mainload(labfile, lablength, labsplitlen, labcolumn, bmifile, demographicfile, diagnosesfile, pharmfile, procedfile, readmitfile, encounterfile, mainfile):
    labs(labfile, lablength, labsplitlen, labcolumn)
    bmi(bmifile)
    if pathlib.Path('./RACE').exists():
        demographic(demographicfile)
    else:
        os.mkdir('./RACE')
        demographic(demographicfile)
    if pathlib.Path('./DIAGNOSIS').exists():
        diagnosis(diagnosesfile)
    else:
        os.mkdir('./DIAGNOSIS')
        diagnosis(diagnosesfile)
    if pathlib.Path('./PHARMA').exists():
        pharmacy(pharmfile)
    else:
        os.mkdir('./PHARMA')
        pharmacy(pharmfile)
    procedure(procedfile)
    readmit(readmitfile)
    encounter(encounterfile)
    if os.path.exists(mainfile):
        os.remove(mainfile)
    outpit = open(mainfile, 'w')
    outpit.write('do ' + labfile + '\r\n')
    outpit.write('do ' + bmifile + '\r\n')
    outpit.write('do ' + demographicfile + '\r\n')
    outpit.write('do ' + diagnosesfile + '\r\n')
    outpit.write('clear')
    outpit.write('\r\n')


observlen = 18000000
column = ['1:3', '9:10', '14:14']

mainload('LabLoad.do', observlen, 5000000, column, 'BMILoad.do', 'DemographicLoad.do', 'DiagnosticLoad.do', 'PharmaLoad.do', 'ProcedureLoad.do', 'ReadmitLoad.do', 'EncounterFile.do', 'MainLoad.do')
