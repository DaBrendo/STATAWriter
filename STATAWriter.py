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
                    r'import delimited ".\Raw\Labs.txt", encoding(utf8) varnames(1) case(upper) colrange(' + y + ') rowrange(' + '' + ':' + str(((x + 1) * argsplitlen)) + ') stringcols(_all)' + '\r\n')
            else:
                outpit.write(
                    r'import delimited ".\Raw\Labs.txt", encoding(utf8) varnames(1) case(upper) colrange(' + y + ') rowrange(' + str(x + (x * argsplitlen)) + ':' + str(x + ((x + 1) * argsplitlen)) + ') stringcols(_all)' + '\r\n')
            outpit.write(r'save ".\Import\Labs\Labs' + str(x + 1) + string.ascii_uppercase[argcolumn.index(y)] + r'.dta", replace' + '\r\n')
            outpit.write('clear')
            outpit.write('\r\n')
        outpit.write('use .\Import\Labs\Labs' + str(x + 1) + 'A' + '.dta' + '\r\n')
        for y in argcolumn:
            if argcolumn.index(y) > 0:
                outpit.write('merge 1:1 _n using .\Import\Labs\Labs' + str(x + 1) + string.ascii_uppercase[
                    argcolumn.index(y)] + '.dta, nogenerate')
                outpit.write('\r\n')
        outpit.write('destring RESULT_ANSWER_TEXT, generate(RESULTS_NUM) force' + '\r\n')
        outpit.write('generate str RESULTS_STR = RESULT_ANSWER_TEXT if RESULTS_NUM ==.' + '\r\n')
        outpit.write('drop RESULT_ANSWER_TEXT' + '\r\n')
        outpit.write('order RESULTS_STR, after(RESULTS_NUM)' + '\r\n')
        outpit.write('replace LOINC_CODE = "" if LOINC_CODE == "?"' + '\r\n')
        outpit.write(r'gen LABCAT = ""' + '\r\n')
        outpit.write(r'rename PROC_NAME PROCEDURE' + '\r\n')
        outpit.write(r'replace PROCEDURE = upper(PROCEDURE)' + '\r\n')
        for file in os.listdir((os.curdir + r'/LABS')):
            filename = os.fsdecode(file)
            if filename.endswith(".csv"):
                with open((os.curdir + r'/LABS' + r'/' + filename), 'r') as csv_file:
                    readin = list(csv.reader(csv_file, delimiter=','))
                lablist = [item for sublist in readin for item in sublist]
                lablist = [x.upper() for x in lablist]
                outpit.write('gen ' + lablist[0] + ' = ""' + '\r\n')
                for z in lablist:
                    outpit.write(r'replace LABCAT = "' + lablist[0] + r'" ' + 'if PROCEDURE == ' + '"' + z + r'"' + '\r\n')
        outpit.write('duplicates drop' + '\r\n')
        outpit.write(r'save ".\Clean\Labs\Labs' + str(x + 1) + r'.dta", replace' + '\r\n')
        outpit.write('clear' + '\r\n')
    for x in range(0, looplen):
        outpit.write(r'use ".\Clean\Labs\Labs' + str(x + 1) + r'.dta"' + '\r\n')
        for file in os.listdir((os.curdir + r'/LABSCRITERIA')):
            filename = os.fsdecode(file)
            if filename.endswith(".csv"):
                with open((os.curdir + r'/LABSCRITERIA' + r'/' + filename), 'r') as csv_file:
                    readin = list(csv.reader(csv_file, delimiter=','))
                lablist = [item for sublist in readin for item in sublist]
                lablist = [x.upper() for x in lablist]
                for y in lablist:
                    outpit.write('replace ' + lablist[0] + ' = "' + lablist[1] + '" if RESULTS_STR == "' + str(
                        y) + '" & LABCAT == "' + lablist[0] + '"' + '\r\n')
        outpit.write('rename ADMTID ADMITID' + '\r\n')
        outpit.write('rename LOINC_CODE LABCODE' + '\r\n')
        outpit.write('rename PROCEDURE LABDES' + '\r\n')
        outpit.write('rename RESULTS_NUM LABNUM' + '\r\n')
        outpit.write('rename RESULTS_STR LABSTR' + '\r\n')
        outpit.write(r'save ".\Merge\Labs\Labs' + str(x + 1) + r'.dta"' + '\r\n')
        outpit.write('clear' + '\r\n')


def bmi(argfile):
    if os.path.exists(argfile):
        os.remove(argfile)
    outpit = open(argfile, 'w')
    outpit.write(r'import delimited ".\Raw\BMI.txt", varnames(1) case(upper) encoding(utf8) stringcols(_all)' + '\r\n')
    outpit.write(r'save ".\Import\BMI.dta", replace' + '\r\n')
    outpit.write('rename HEIGHT_CENTIMETERS HEIGHT' + '\r\n')
    outpit.write('rename WEIGHT_KILOGRAMS WEIGHT' + '\r\n')
    outpit.write('rename CALCULATED_BMI BMI' + '\r\n')
    outpit.write('rename ADMTID ADMITID' + '\r\n')
    outpit.write('destring WEIGHT, replace' + '\r\n')
    outpit.write('destring BMI, replace' + '\r\n')
    outpit.write('duplicates drop' + '\r\n')
    outpit.write(r'save ".\Clean\BMI.dta", replace' + '\r\n')
    outpit.write('clear' + '\r\n')


def demographic(argfile):
    if os.path.exists(argfile):
            os.remove(argfile)
    outpit = open(argfile, 'w')
    outpit.write(
        r'import delimited ".\Raw\Demographics.txt", varnames(1) case(upper) encoding(utf8) stringcols(_all)' + '\r\n')
    outpit.write(r'save ".\Import\Demographics.dta", replace' + '\r\n')
    outpit.write(r'rename ESRI_GROUP ESRI' + '\r\n')
    outpit.write(r'drop ETHNICITY' + '\r\n')
    outpit.write(r'replace RACE1 = upper(RACE1)' + '\r\n')
    outpit.write(r'replace RACE2 = upper(RACE2)' + '\r\n')
    outpit.write(r'gen str RACE1FIX = ""' + '\r\n')
    outpit.write(r'gen str RACE2FIX = ""' + '\r\n')
    outpit.write(r'gen str RACE = ""' + '\r\n')
    for file in os.listdir((os.curdir + r'/RACE')):
        filename = os.fsdecode(file)
        if filename.endswith(".csv"):
            with open((os.curdir + r'/DEMOGRAPHICS' + r'/' + filename), 'r') as csv_file:
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
    for file in os.listdir((os.curdir + r'/RACE')):
        filename = os.fsdecode(file)
        if filename.endswith(".csv"):
            with open((os.curdir + r'/Race' + r'/' + filename), 'r') as csv_file:
                readin = list(csv.reader(csv_file, delimiter=','))
            racelist = [item for sublist in readin for item in sublist]
            racelist = [x.upper() for x in racelist if x != "OTHER"]
            outpit.write(r'replace RACE = "' + racelist[0] + r'" ' + 'if RACE1FIX == "' + racelist[0] + r'" & RACE2FIX == "OTHER"' + '\r\n')
            outpit.write(r'replace RACE = "' + racelist[0] + r'" ' + 'if RACE1FIX == "OTHER" & RACE2FIX == "' + racelist[0] + r'"' + '\r\n')
    outpit.write(r'replace RACE = "MULTIRACIAL" if RACE == ""' + '\r\n')
    outpit.write(r'drop RACE1FIX' + '\r\n')
    outpit.write(r'drop RACE2FIX' + '\r\n')
    outpit.write(r'drop RACE1' + '\r\n')
    outpit.write(r'drop RACE2' + '\r\n')
    outpit.write(r'drop if ESRI == ""' + '\r\n')
    outpit.write(r'drop if ESRI == "?"' + '\r\n')
    outpit.write('rename ADMTID ADMITID' + '\r\n')
    outpit.write('duplicates drop' + '\r\n')
    outpit.write('save ".\Clean\Demographics.dta", replace' + '\r\n')
    outpit.write('clear' + '\r\n')


def diagnosis(argfile, typer="ORIG"):
    if os.path.exists(argfile):
        os.remove(argfile)
    outpit = open(argfile, 'w')
    if typer == "ORIG":
        outpit.write(
            r'import delimited ".\Raw\Diagnoses.txt", varnames(1) case(upper) encoding(utf8) stringcols(_all)' + '\r\n')
        outpit.write('save ".\Import\Diagnoses.dta", replace' + '\r\n')
    elif typer == "FOLLOW":
        outpit.write(
            r'import delimited ".\Raw\Diagnoses_Followup.txt", varnames(1) case(upper) encoding(utf8) stringcols(_all)' + '\r\n')
        outpit.write('save ".\Import\Diagnoses_Followup.dta", replace' + '\r\n')
    outpit.write('duplicates drop' + '\r\n')
    outpit.write(r'drop DX DX_CODETYPE ORIGDX' + '\r\n')
    outpit.write(r'replace DXDESC = upper(DXDESC)' + '\r\n')
    outpit.write(r'gen DIAGCAT = ""' + '\r\n')
    if typer == "ORIG":
        outpit.write(r'gen DIAGTYPE = "ORIG"' + '\r\n')
    if typer == "FOLLOW":
        outpit.write(r'gen DIAGTYPE = "FOLLOWUP"' + '\r\n')
    for file in os.listdir((os.curdir + r'/DIAGNOSES')):
        filename = os.fsdecode(file)
        if filename.endswith(".csv"):
            with open((os.curdir + r'/DIAGNOSES' + r'/' + filename), 'r') as csv_file:
                readin = list(csv.reader(csv_file, delimiter=','))
            diaglist = [item for sublist in readin for item in sublist]
            for x in diaglist:
                outpit.write(r'replace DIAGCAT = "' + diaglist[0] + r'" ' + 'if DXDESC == "' + x + r'"' + '\r\n')
    outpit.write(r'drop if DIAGCAT == ""' + '\r\n')
    outpit.write(r'drop if DIAGCAT == "BLEED DISORDER"' + '\r\n')
    outpit.write(r'drop if DIAGCAT == "CLOT DISORDER"' + '\r\n')
    outpit.write(r'drop if DIAGCAT == "PRIOR VTE"' + '\r\n')
    outpit.write('rename ADMTID ADMITID' + '\r\n')
    outpit.write('rename DXSEQ DIAGNUM' + '\r\n')
    outpit.write('rename DXDESC DIAGDES' + '\r\n')
    outpit.write('drop DIAG_CYCLE_CODE' + '\r\n')
    if typer == "ORIG":
        outpit.write('save ".\Clean\Diagnoses.dta", replace' + '\r\n')
    elif typer == "FOLLOW":
        outpit.write('save ".\Clean\Diagnoses_Followup.dta", replace' + '\r\n')
    outpit.write('duplicates drop' + '\r\n')
    outpit.write('drop DIAGNUM' + '\r\n')
    outpit.write('sort ADMITID DIAGCAT' + '\r\n')
    outpit.write('replace DIAGTYPE = "ORIG1" if DIAGTYPE == "ORIG"' + '\r\n')
    outpit.write('replace DIAGTYPE = "FOLLOWUP1_1" if DIAGTYPE == "FOLLLOWUP"' + '\r\n')
    outpit.write('replace DIAGTYPE = "FOLLOWUP2_1" if DIAGTYPE == "FOLLOWUP2"' + '\r\n')
    for z in range(2):
        for y in range(1, 10):
            outpit.write('sort ADMITID DIAGTYPE' + '\r\n')
            outpit.write('replace DIAGTYPE = "ORIG' + str((y + 1)) + '" if DIAGTYPE == "ORIG' + str(y) + '" & ADMITID == ADMITID[_n - 1] & DIAGTYPE == DIAGTYPE[_n - 1]' + '\r\n')
    for z in range(2):
        for y in range(1, 10):
            outpit.write('sort ADMITID DIAGTYPE' + '\r\n')
            outpit.write('replace DIAGTYPE = "FOLLOWUP1_' + str((y + 1)) + '" if DIAGTYPE == "FOLLOWUP1_' + str(y) + '" & ADMITID == ADMITID[_n - 1] & DIAGTYPE == DIAGTYPE[_n - 1]' + '\r\n')
    for z in range(2):
        for y in range(1, 10):
            outpit.write('sort ADMITID DIAGTYPE' + '\r\n')
            outpit.write('replace DIAGTYPE = "FOLLOWUP2_' + str((y + 1)) + '" if DIAGTYPE == "FOLLOWUP2_' + str(y) + '" & ADMITID == ADMITID[_n - 1] & DIAGTYPE == DIAGTYPE[_n - 1]' + '\r\n')
        outpit.write('sort ADMITID DIAGTYPE' + '\r\n')
    if typer == "ORIG":
        outpit.write('save ".\Merge\Diagnosis.dta", replace' + '\r\n')
    elif typer == "FOLLOW":
        outpit.write('save ".\Merge\Diagnosis_Followup.dta", replace' + '\r\n')
    outpit.write('clear' + '\r\n')


def pharmacy(argfile):
    if os.path.exists(argfile):
        os.remove(argfile)
    outpit = open(argfile, 'w')
    outpit.write(
        r'import delimited ".\Raw\Pharmacy.txt", varnames(1) case(upper) encoding(utf8) stringcols(_all)' + '\r\n')
    outpit.write('save ".\Import\Pharmacy.dta", replace' + '\r\n')
    outpit.write('duplicates drop' + '\r\n')
    outpit.write(r'drop ADMIN_TIME PHRMMNEM GENERICPHRMMNEM THERCLSGRP GENNAMEGRP SPECNAMEGRP RX_NDC' + '\r\n')
    outpit.write('rename REL_ADMIN_DATE MEDDATE' + '\r\n')
    outpit.write('drop MEDTYPE' + '\r\n')
    outpit.write('rename MED MEDTYPE' + '\r\n')
    outpit.write(r'replace MEDTYPE = upper(MEDTYPE)' + '\r\n')
    outpit.write('rename ROUTE MEDROUTE' + '\r\n')
    outpit.write('rename DOSE MEDDOSE' + '\r\n')
    outpit.write(r'gen MEDCAT = ""' + '\r\n')
    for file in os.listdir((os.curdir + r'/PHARMACY')):
        filename = os.fsdecode(file)
        if filename.endswith(".csv"):
            with open((os.curdir + r'/PHARMACY' + r'/' + filename), 'r') as csv_file:
                readin = list(csv.reader(csv_file, delimiter=','))
            medlist = [item for sublist in readin for item in sublist]
            medlist = [x.upper() for x in medlist]
            for x in medlist:
                outpit.write(r'replace MEDCAT = "' + medlist[0] + r'" ' + 'if MEDTYPE == "' + x + r'"' + '\r\n')
    outpit.write(r'drop if MEDCAT == ""' + '\r\n')
    outpit.write('duplicates drop' + '\r\n')
    outpit.write('save ".\Clean\Pharmacy.dta", replace' + '\r\n')
    outpit.write(r'drop if MEDDATE == ""' + '\r\n')
    outpit.write('rename ADMTID ADMITID' + '\r\n')
    outpit.write('sort ADMITID MEDDATE' + '\r\n')
    for z in range(3):
        for x in range(-25, 125):
            outpit.write('replace MEDDATE = "' + str(x) + '_0" if MEDDATE == "' + str(x) + '"' + '\r\n')
            for y in range(9):
                outpit.write('sort ADMITID MEDDATE' + '\r\n')
                outpit.write('replace MEDDATE = "' + str(x) + '_' + str((y + 1)) + '" if MEDDATE == "' + str(x) + '_' + str(y) + '" & ADMITID == ADMITID[_n - 1] & MEDDATE == MEDDATE[_n - 1]' + '\r\n')
            outpit.write('sort ADMITID MEDDATE' + '\r\n')
        outpit.write('sort ADMITID MEDDATE' + '\r\n')
    outpit.write('save ".\Merge\Pharmacy.dta", replace' + '\r\n')
    outpit.write('clear' + '\r\n')


def procedure(argfile):
    if os.path.exists(argfile):
        os.remove(argfile)
    outpit = open(argfile, 'w')
    outpit.write(
        r'import delimited ".\Raw\Procedures.txt", varnames(1) case(upper) encoding(utf8) stringcols(_all)' + '\r\n')
    outpit.write('save ".\Import\Procedures.dta", replace' + '\r\n')
    outpit.write('duplicates drop' + '\r\n')
    outpit.write(r'drop REL_SERVICE_DAY ORIGPX PX' + '\r\n')
    outpit.write(r'replace PX_DESC = upper(PX_DESC)' + '\r\n')
    outpit.write(r'gen PROCAT = ""' + '\r\n')
    for file in os.listdir((os.curdir + r'/PROCEDURES')):
        filename = os.fsdecode(file)
        if filename.endswith(".csv"):
            with open((os.curdir + r'/PROCEDURES' + r'/' + filename), 'r') as csv_file:
                readin = list(csv.reader(csv_file, delimiter=','))
            proclist = [item for sublist in readin for item in sublist]
            proclist = [x.upper() for x in proclist]
            for x in proclist:
                outpit.write(r'replace PROCAT = "' + proclist[0] + r'" ' + 'if PX_DESC == "' + x + r'"' + '\r\n')
    outpit.write(r'drop if PROCAT == ""' + '\r\n')
    outpit.write('rename PROC_SEQ_NUM PROCNUM' + '\r\n')
    outpit.write('rename PX_DESC PROCDES' + '\r\n')
    outpit.write('rename PROCAT PROCCAT' + '\r\n')
    outpit.write('drop PX_CODETYPE' + '\r\n')
    outpit.write('rename ADMTID ADMITID' + '\r\n')
    outpit.write('save ".\Clean\Procedures.dta", replace' + '\r\n')
    outpit.write('contract ADMITID' + '\r\n')
    outpit.write('drop _freq' + '\r\n')
    outpit.write('save ".\Dictionary\ID.dta", replace' + '\r\n')
    outpit.write('clear' + '\r\n')


def readmit(argfile):
    if os.path.exists(argfile):
        os.remove(argfile)
    outpit = open(argfile, 'w')
    outpit.write(
        r'import delimited ".\Raw\Readmit.txt", varnames(1) case(upper) encoding(utf8) stringcols(_all)' + '\r\n')
    outpit.write(r'save ".\Import\Readmit.dta", replace' + '\r\n')
    outpit.write('duplicates drop' + '\r\n')
    outpit.write('drop if READMIT_DAYS == ""' + '\r\n')
    outpit.write('drop if READMIT_DAYS == "?"' + '\r\n')
    outpit.write('rename ADMTID ADMITID' + '\r\n')
    outpit.write('rename ADMIT_YEAR ADMITYR' + '\r\n')
    outpit.write(r'save ".\Clean\Readmit.dta", replace' + '\r\n')
    outpit.write('clear' + '\r\n')


def encounter(argfile):
    if os.path.exists(argfile):
        os.remove(argfile)
    outpit = open(argfile, 'w')
    outpit.write(
        r'import delimited ".\Raw\Encounters.txt", varnames(1) case(upper) encoding(utf8) stringcols(_all)' + '\r\n')
    outpit.write('save ".\Import\Encounters.dta", replace' + '\r\n')
    outpit.write('duplicates drop' + '\r\n')
    outpit.write('rename ADMTID ADMITID' + '\r\n')
    outpit.write('drop ENCTYPE' + '\r\n')
    outpit.write('drop PAT_ZIP_MASKED' + '\r\n')
    outpit.write('drop DRG' + '\r\n')
    outpit.write('drop DRG_TYPE' + '\r\n')
    outpit.write('drop APRDRG' + '\r\n')
    outpit.write('drop HCA_ADM_CLASS' + '\r\n')
    outpit.write('rename HCA_DISCH_DISPO DCHARGECODE' + '\r\n')
    outpit.write('rename HCA_DISCH_DISPO_DESC DCHARGEDES' + '\r\n')
    outpit.write('rename HCA_ADM_SRC ADMITSOURCE' + '\r\n')
    outpit.write('rename AGEYRS AGE' + '\r\n')
    outpit.write('rename REL_DISCHARGE_DAY DCHARGEDAY' + '\r\n')
    outpit.write('save ".\Clean\Encounters.dta", replace' + '\r\n')
    outpit.write('clear' + '\r\n')


def mainload(labfile, lablength, labsplitlen, labcolumn, bmifile, demographicfile, diagnosesfile, pharmfile, procedfile, readmitfile, encounterfile, mainfile, diagnoses2file):
    if pathlib.Path('./PROCEDURES').exists():
        procedure(procedfile)
    else:
        os.mkdir('./PROCEDURES')
        procedure(procedfile)
    labs(labfile, lablength, labsplitlen, labcolumn)
    bmi(bmifile)
    if pathlib.Path('./RACE').exists():
        demographic(demographicfile)
    else:
        os.mkdir('./RACE')
        demographic(demographicfile)
    if pathlib.Path('./DIAGNOSES').exists():
        diagnosis(diagnosesfile)
        diagnosis(diagnoses2file, typer="FOLLOW")
    else:
        os.mkdir('./DIAGNOSIS')
        diagnosis(diagnosesfile)
        diagnosis(diagnoses2file, typer="FOLLOW")
    if pathlib.Path('./PHARMACY').exists():
        pharmacy(pharmfile)
    else:
        os.mkdir('./PHARMA')
        pharmacy(pharmfile)
    readmit(readmitfile)
    encounter(encounterfile)
    if os.path.exists(mainfile):
        os.remove(mainfile)
    outpit = open(mainfile, 'w')
    outpit.write('do ' + labfile + '\r\n')
    outpit.write('do ' + bmifile + '\r\n')
    outpit.write('do ' + demographicfile + '\r\n')
    outpit.write('do ' + diagnosesfile + '\r\n')
    outpit.write('do ' + diagnoses2file + '\r\n')
    outpit.write('do ' + procedfile + '\r\n')
    outpit.write('do ' + encounterfile + '\r\n')
    outpit.write('do ' + readmitfile + '\r\n')
    outpit.write('do ' + pharmfile + '\r\n')
    outpit.write('clear' + '\r\n')


observlen = 18000000
column = ['1:3', '9:10', '14:14']

mainload('LabLoad.do', observlen, 5000000, column, 'BMILoad.do', 'DemographicLoad.do', 'DiagnosisLoad.do',
         'PharmaLoad.do', 'ProcedureLoad.do', 'ReadmitLoad.do', 'EncounterFile.do', 'MainLoad.do', 'DiagnosisFollowLoad.do')