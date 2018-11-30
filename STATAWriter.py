import string
import os
import math
import csv
import pathlib


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
            outpit.write('clear' + '\r\n')
        outpit.write('use .\Import\Labs\Labs' + str(x + 1) + 'A' + '.dta' + '\r\n')
        for y in argcolumn:
            if argcolumn.index(y) > 0:
                outpit.write('merge 1:1 _n using .\Import\Labs\Labs' + str(x + 1) + string.ascii_uppercase[
                    argcolumn.index(y)] + '.dta, nogenerate' + '\r\n')
        outpit.write('duplicates drop' + '\r\n')
        outpit.write('rename ADMTID ADMITID' + '\r\n')
        outpit.write('sort ADMITID' + '\r\n')
        outpit.write(r'merge m:1 ADMITID using ".\Dictionary\ID.dta", keep(match) nogenerate' + '\r\n')
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
        outpit.write('rename LOINC_CODE LABCODE' + '\r\n')
        outpit.write('rename PROCEDURE LABDES' + '\r\n')
        outpit.write('rename RESULTS_NUM LABNUM' + '\r\n')
        outpit.write('rename RESULTS_STR LABSTR' + '\r\n')
        outpit.write(r'save ".\Merge\Labs\Labs' + str(x + 1) + r'.dta", replace' + '\r\n')
        outpit.write('clear' + '\r\n')


def bmi(argfile):
    if os.path.exists(argfile):
        os.remove(argfile)
    outpit = open(argfile, 'w')
    outpit.write(r'import delimited ".\Raw\BMI.txt", varnames(1) case(upper) encoding(utf8) stringcols(_all)' + '\r\n')
    outpit.write(r'save ".\Import\BMI.dta", replace' + '\r\n')
    outpit.write('duplicates drop' + '\r\n')
    outpit.write('rename ADMTID ADMITID' + '\r\n')
    outpit.write('sort ADMITID' + '\r\n')
    outpit.write(r'merge m:1 ADMITID using ".\Dictionary\ID.dta", keep(match) nogenerate' + '\r\n')
    outpit.write('rename HEIGHT_CENTIMETERS HEIGHT' + '\r\n')
    outpit.write('rename WEIGHT_KILOGRAMS WEIGHT' + '\r\n')
    outpit.write('rename CALCULATED_BMI BMI' + '\r\n')
    outpit.write('destring WEIGHT, replace' + '\r\n')
    outpit.write('destring BMI, replace' + '\r\n')
    outpit.write('destring HEIGHT, replace' + '\r\n')
    outpit.write('drop PTID' + '\r\n')
    outpit.write(r'save ".\Clean\BMI.dta", replace' + '\r\n')
    outpit.write('sort ADMITID' + '\r\n')
    outpit.write('save ".\Merge\BMI.dta", replace' + '\r\n')
    outpit.write('clear' + '\r\n')


def demographic(argfile):
    if os.path.exists(argfile):
            os.remove(argfile)
    outpit = open(argfile, 'w')
    outpit.write(
        r'import delimited ".\Raw\Demographics.txt", varnames(1) case(upper) encoding(utf8) stringcols(_all)' + '\r\n')
    outpit.write(r'save ".\Import\Demographics.dta", replace' + '\r\n')
    outpit.write('duplicates drop' + '\r\n')
    outpit.write('rename ADMTID ADMITID' + '\r\n')
    outpit.write('sort ADMITID' + '\r\n')
    outpit.write(r'merge m:1 ADMITID using ".\Dictionary\ID.dta", keep(match) nogenerate' + '\r\n')
    outpit.write(r'rename ESRI_GROUP ESRI' + '\r\n')
    outpit.write(r'drop ETHNICITY' + '\r\n')
    outpit.write('drop PTID' + '\r\n')
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
    outpit.write('duplicates drop' + '\r\n')
    outpit.write('save ".\Clean\Demographics.dta", replace' + '\r\n')
    outpit.write('sort ADMITID' + '\r\n')
    outpit.write('save ".\Merge\Demographics.dta", replace' + '\r\n')
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
    outpit.write('rename ADMTID ADMITID' + '\r\n')
    outpit.write('sort ADMITID' + '\r\n')
    outpit.write(r'merge m:1 ADMITID using ".\Dictionary\ID.dta", keep(match) nogenerate' + '\r\n')
    outpit.write(r'drop DX DX_CODETYPE ORIGDX PTID' + '\r\n')
    outpit.write(r'replace DXDESC = upper(DXDESC)' + '\r\n')
    outpit.write(r'gen DIAGCAT = ""' + '\r\n')
    if typer == "ORIG":
        outpit.write(r'gen DIAGTYPE = "ORIG1"' + '\r\n')
    if typer == "FOLLOW":
        outpit.write(r'gen DIAGTYPE = "FOLLOWUP1"' + '\r\n')
    for file in os.listdir((os.curdir + r'/DIAGNOSES')):
        filename = os.fsdecode(file)
        if filename.endswith(".csv"):
            with open((os.curdir + r'/DIAGNOSES' + r'/' + filename), 'r') as csv_file:
                readin = list(csv.reader(csv_file, delimiter=','))
            diaglist = [item for sublist in readin for item in sublist]
            for x in diaglist:
                outpit.write(r'replace DIAGCAT = "' + diaglist[0] + r'" ' + 'if DXDESC == "' + x + r'"' + '\r\n')
    outpit.write(r'drop if DIAGCAT == ""' + '\r\n')
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
    outpit.write('sort ADMITID DIAGTYPE' + '\r\n')
    if typer == "ORIG":
        for z in range(4):
            for y in range(1, 10):
                outpit.write('sort ADMITID DIAGTYPE' + '\r\n')
                outpit.write('replace DIAGTYPE = "ORIG' + str((y + 1)) + '" if DIAGTYPE == "ORIG' + str(y) + '" & ADMITID == ADMITID[_n - 1] & DIAGTYPE == DIAGTYPE[_n - 1]' + '\r\n')
                outpit.write('sort ADMITID DIAGTYPE' + '\r\n')
    elif typer == "FOLLOW":
        for z in range(4):
            for y in range(1, 10):
                outpit.write('sort ADMITID DIAGTYPE' + '\r\n')
                outpit.write('replace DIAGTYPE = "FOLLOWUP' + str((y + 1)) + '" if DIAGTYPE == "FOLLOWUP' + str(y) + '" & ADMITID == ADMITID[_n - 1] & DIAGTYPE == DIAGTYPE[_n - 1]' + '\r\n')
                outpit.write('sort ADMITID DIAGTYPE' + '\r\n')
    outpit.write('sort ADMITID DIAGTYPE' + '\r\n')
    outpit.write('reshape wide DIAGDES PADMIT DIAGCAT, i(ADMITID) j(DIAGTYPE) string' + '\r\n')
    if typer == "ORIG":
        for y in range(1, 10):
            outpit.write('capture rename DIAGDESORIG' + str(y) + ' ORIG' + str(y) + 'DES' + '\r\n')
            outpit.write('capture rename PADMITORIG' + str(y) + ' ORIG' + str(y) + 'ADMITD' + '\r\n')
            outpit.write('capture rename DIAGCATORIG' + str(y) + ' ORIG' + str(y) + 'CAT' + '\r\n')
    elif typer == "FOLLOW":
        for y in range(1, 10):
            outpit.write('capture rename DIAGDESFOLLOWUP' + str(y) + ' FLWUP' + str(y) + 'DES' + '\r\n')
            outpit.write('capture rename PADMITFOLLOWUP' + str(y) + ' FLWUP' + str(y) + 'ADMITD' + '\r\n')
            outpit.write('capture rename DIAGCATFOLLOWUP' + str(y) + ' FLWUP' + str(y) + 'CAT' + '\r\n')
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
    outpit.write('rename ADMTID ADMITID' + '\r\n')
    outpit.write('sort ADMITID' + '\r\n')
    outpit.write(r'merge m:1 ADMITID using ".\Dictionary\ID.dta", keep(match) nogenerate' + '\r\n')
    outpit.write(r'drop ADMIN_TIME PHRMMNEM GENERICPHRMMNEM THERCLSGRP GENNAMEGRP SPECNAMEGRP RX_NDC PTID' + '\r\n')
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
    outpit.write('sort ADMITID MEDDATE' + '\r\n')
    outpit.write('duplicates drop' + '\r\n')
    outpit.write('save ".\Clean\Pharmacy.dta", replace' + '\r\n')
    outpit.write('contract ADMITID MEDCAT' + '\r\n')
    outpit.write('drop _freq' + '\r\n')
    outpit.write('gen MEDNUM = "MED1"' + '\r\n')
    outpit.write('sort ADMITID MEDNUM' + '\r\n')
    for z in range(5):
        for y in range(1, 10):
            outpit.write('sort ADMITID MEDNUM' + '\r\n')
            outpit.write('replace MEDNUM = "MED' + str((y + 1)) + '" if MEDNUM == "MED' + str(y) + '" & ADMITID == ADMITID[_n - 1] & MEDNUM == MEDNUM[_n - 1]' + '\r\n')
    outpit.write('reshape wide MEDCAT, i(ADMITID) j(MEDNUM) string' + '\r\n')
    for y in range(1, 10):
        outpit.write('capture rename MEDCATMED' + str(y) + ' MED' + str(y) + '\r\n')
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
    outpit.write('rename ADMTID ADMITID' + '\r\n')
    outpit.write('sort ADMITID' + '\r\n')
    outpit.write(r'drop REL_SERVICE_DAY ORIGPX PX PTID PX_CODETYPE' + '\r\n')
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
    outpit.write('save ".\Clean\Procedures.dta", replace' + '\r\n')
    outpit.write('contract ADMITID' + '\r\n')
    outpit.write('drop _freq' + '\r\n')
    outpit.write('save ".\Dictionary\ID.dta", replace' + '\r\n')
    outpit.write('clear' + '\r\n')
    outpit.write('use ".\Clean\Procedures.dta"' + '\r\n')
    outpit.write('sort ADMITID PROCNUM' + '\r\n')
    outpit.write('drop if ADMITID == ADMITID[_n - 1] & PROCNUM == PROCNUM[_n - 1] & PROCCAT == "KNEE REV"' + '\r\n')
    outpit.write('drop if ADMITID == ADMITID[_n + 1] & PROCNUM == PROCNUM[_n + 1] & PROCCAT == "KNEE REV"' + '\r\n')
    outpit.write('replace PROCNUM = "1000" if PROCDES == "BLOOD TRANSFUSION SERVICE"' + '\r\n')
    outpit.write('sort ADMITID PROCNUM' + '\r\n')
    outpit.write('gen PROC = "PROC1"' + '\r\n')
    outpit.write('replace PROC = "PROC1000" if PROCDES == "BLOOD TRANSFUSION SERVICE"' + '\r\n')
    outpit.write('destring PROCNUM, replace' + '\r\n')
    outpit.write('sort ADMITID PROCNUM' + '\r\n')
    for z in range(4):
        for y in range(1, 10):
            outpit.write('sort ADMITID PROC' + '\r\n')
            outpit.write('replace PROC = "PROC' + str(y) + '" if ADMITID == ADMITID[_n - 1] & PROC == PROC[_n - 1] & PROCDES != "BLOOD TRANSFUSION SERVICE"' + '\r\n')
    for z in range(5):
        for x in range(1, 14):
            if x < 10:
                outpit.write('sort ADMITID PROCNUM PROC' + '\r\n')
                outpit.write('replace PROC = "PROC100' + str(x) + '" if ADMITID == ADMITID[_n - 1] & PROC == PROC[_n - 1] & PROCDES == "BLOOD TRANSFUSION SERVICE"' + '\r\n')
            else:
                outpit.write('sort ADMITID PROCNUM PROC' + '\r\n')
                outpit.write('replace PROC = "PROC10' + str(x) + '" if ADMITID == ADMITID[_n - 1] & PROC == PROC[_n - 1] & PROCDES == "BLOOD TRANSFUSION SERVICE"' + '\r\n')
    outpit.write('reshape wide PROCNUM PROCDES PROCCAT, i(ADMITID) j(PROC) string' + '\r\n')
    for y in range(1, 14):
        outpit.write('capture rename PROCDESPROC' + str(y) + ' PROC' + str(y) + 'DES' + '\r\n')
        outpit.write('capture rename PROCNUMPROC' + str(y) + ' PROC' + str(y) + 'NUM' + '\r\n')
        outpit.write('capture rename PROCCATPROC' + str(y) + ' PROC' + str(y) + 'CAT' + '\r\n')
    outpit.write('gen TRNSFUSNUM = 0' + '\r\n')
    for x in range(1, 14):
        if x < 10:
            outpit.write('capture replace TRNSFUSNUM = TRNSFUSNUM + 1 if PROCDESPROC100' + str(x) + ' == "BLOOD TRANSFUSION SERVICE"' + '\r\n')
        else:
            outpit.write('capture replace TRNSFUSNUM = TRNSFUSNUM + 1 if PROCDESPROC10' + str(x) + ' == "BLOOD TRANSFUSION SERVICE"' + '\r\n')
    for x in range(1, 10):
        outpit.write('capture replace TRNSFUSNUM = TRNSFUSNUM + 1 if PROC' + str(x) + 'CAT == "TRANSFUSE"' + '\r\n')
    for x in range(0, 14):
        if x < 10:
            outpit.write('capture drop PROCNUMPROC100' + str(x) + ' PROCDESPROC100' + str(x) + ' PROCCATPROC100' + str(x) + '\r\n')
        else:
            outpit.write('capture drop PROCNUMPROC10' + str(x) + ' PROCDESPROC10' + str(x) + ' PROCCATPROC10' + str(x) + '\r\n')
    outpit.write('save ".\Merge\Procedures.dta", replace' + '\r\n')
    outpit.write('keep if PROC1NUM == 2' + '\r\n')
    outpit.write('keep ADMITID' + '\r\n')
    outpit.write('rename ADMITID ADMTID' + '\r\n')
    outpit.write('merge 1:m ADMTID using ".\Import\Procedures.dta", keep(master match) nogenerate' + '\r\n')
    outpit.write('keep if PROC_SEQ_NUM == "1"' + '\r\n')
    outpit.write('duplicates drop' + '\r\n')
    outpit.write('rename ADMTID ADMITID' + '\r\n')
    outpit.write('sort ADMITID' + '\r\n')
    outpit.write(r'drop REL_SERVICE_DAY ORIGPX PX PTID PX_CODETYPE' + '\r\n')
    outpit.write(r'replace PX_DESC = upper(PX_DESC)' + '\r\n')
    outpit.write('rename PROC_SEQ_NUM PROCNUM' + '\r\n')
    outpit.write('rename PX_DESC PROCDES' + '\r\n')
    outpit.write('save ".\Dictionary\FirstUnrelatedProcedures.dta", replace' + '\r\n')
    outpit.write('clear' + '\r\n')


def readmit(argfile):
    if os.path.exists(argfile):
        os.remove(argfile)
    outpit = open(argfile, 'w')
    outpit.write(
        r'import delimited ".\Raw\Readmit.txt", varnames(1) case(upper) encoding(utf8) stringcols(_all)' + '\r\n')
    outpit.write(r'save ".\Import\Readmit.dta", replace' + '\r\n')
    outpit.write('duplicates drop' + '\r\n')
    outpit.write('rename ADMTID ADMITID' + '\r\n')
    outpit.write('sort ADMITID' + '\r\n')
    outpit.write(r'merge m:1 ADMITID using ".\Dictionary\ID.dta", keep(match) nogenerate' + '\r\n')
    outpit.write('replace READMIT_DAYS = "" if READMIT_DAYS == "?"' + '\r\n')
    outpit.write('rename ADMIT_YEAR ADMITYR' + '\r\n')
    outpit.write('rename READMIT_DAYS READMIT' + '\r\n')
    outpit.write('gen READMITNUM = "READMIT1"' + '\r\n')
    outpit.write('drop COMPANY_CODE PTID' + '\r\n')
    outpit.write('sort ADMITID ADMITYR READMIT' + '\r\n')
    outpit.write('duplicates drop' + '\r\n')
    outpit.write(r'save ".\Clean\Readmit.dta", replace' + '\r\n')
    outpit.write('destring ADMITYR, replace' + '\r\n')
    outpit.write('destring READMIT, replace' + '\r\n')
    outpit.write('sort ADMITID ADMITYR READMIT' + '\r\n')
    for z in range(5):
        for y in range(1, 10):
            outpit.write('sort ADMITID ADMITYR READMIT' + '\r\n')
            outpit.write('replace READMITNUM = "READMIT' + str((y + 1)) + '" if READMITNUM == "READMIT' + str(
                y) + '" & ADMITID == ADMITID[_n - 1] & READMITNUM == READMITNUM[_n - 1]' + '\r\n')
    outpit.write('gen PRIORADMIT = ""' + '\r\n')
    outpit.write('replace PRIORADMIT = "TRUE" if READMIT < 0' + '\r\n')
    outpit.write('replace PRIORADMIT = "FALSE" if PRIORADMIT == ""' + '\r\n')
    outpit.write('replace READMIT = . if READMIT < 0' + '\r\n')
    outpit.write('reshape wide ADMITYR READMIT PRIORADMIT, i(ADMITID) j(READMITNUM) string' + '\r\n')
    for y in range(1, 10):
        outpit.write('capture rename ADMITYRREADMIT' + str(y) + ' READMIT' + str(y) + 'YR' + '\r\n')
        outpit.write('capture rename READMITREADMIT' + str(y) + ' READMIT' + str(y) + 'DAY' + '\r\n')
        outpit.write('capture rename PRIORADMITREADMIT' + str(y) + ' PRIORADMIT' + str(y) + '\r\n')
    for y in range(2, 10):
        outpit.write('capture drop PRIORADMIT' + str(y) + '\r\n')
    outpit.write('rename PRIORADMIT1 PRIORADMIT' + '\r\n')
    outpit.write(r'save ".\Merge\Readmit.dta", replace' + '\r\n')
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
    outpit.write('sort ADMITID' + '\r\n')
    outpit.write(r'merge m:1 ADMITID using ".\Dictionary\ID.dta", keep(match) nogenerate' + '\r\n')
    outpit.write('drop PAYER_TYPE HCA_DISCH_DISPO_DESC ENCTYPE PTID PAT_ZIP_MASKED DRG DRG_TYPE HCA_ADM_CLASS' + '\r\n')
    outpit.write('drop if APRDRG == "?"' + '\r\n')
    outpit.write('drop APRDRG' + '\r\n')
    outpit.write('rename HCA_DISCH_DISPO DCHRGECD' + '\r\n')
    outpit.write('rename HCA_ADM_SRC ADMITSRCE' + '\r\n')
    outpit.write('rename AGEYRS AGE' + '\r\n')
    outpit.write('destring AGE, replace' + '\r\n')
    outpit.write('rename REL_DISCHARGE_DAY DCHRGEDY' + '\r\n')
    outpit.write('destring DCHRGEDY, replace' + '\r\n')
    outpit.write('rename ADMIT_YEAR ADMITYR' + '\r\n')
    outpit.write('destring ADMITYR, replace' + '\r\n')
    outpit.write('save ".\Clean\Encounters.dta", replace' + '\r\n')
    outpit.write('duplicates drop' + '\r\n')
    outpit.write('save ".\Merge\Encounters.dta", replace' + '\r\n')
    outpit.write('clear' + '\r\n')


def mainload(bmifile='BMILoad.do', demographicfile='DemographicLoad.do', diagnosesfile='DiagnosisLoad.do',
             pharmfile='PharmaLoad.do', procedfile='ProcedureLoad.do', readmitfile='ReadmitLoad.do',
             encounterfile='EncounterLoad.do', mainfile='MainLoad.do', diagnoses2file='DiagnosisFollowLoad.do'):
    if pathlib.Path('./PROCEDURES').exists():
        procedure(procedfile)
    else:
        os.mkdir('./PROCEDURES')
        procedure(procedfile)
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
    readmit(readmitfile)
    if pathlib.Path('./PHARMACY').exists():
        pharmacy(pharmfile)
    else:
        os.mkdir('./PHARMA')
        pharmacy(pharmfile)
    encounter(encounterfile)
    if os.path.exists(mainfile):
        os.remove(mainfile)
    outpit = open(mainfile, 'w')
    outpit.write('do ' + procedfile + '\r\n')
    outpit.write('do ' + bmifile + '\r\n')
    outpit.write('do ' + demographicfile + '\r\n')
    outpit.write('do ' + diagnosesfile + '\r\n')
    outpit.write('do ' + diagnoses2file + '\r\n')
    outpit.write('do ' + encounterfile + '\r\n')
    outpit.write('do ' + readmitfile + '\r\n')
    outpit.write('do ' + pharmfile + '\r\n')
    outpit.write('clear' + '\r\n')
    outpit.write('use ".\Dictionary\ID.dta"' + '\r\n')
    outpit.write('merge 1:1 ADMITID using ".\Merge\BMI.dta", keep(master match) nogenerate' + '\r\n')
    outpit.write('merge 1:1 ADMITID using ".\Merge\Demographics.dta", keep(master match) nogenerate' + '\r\n')
    outpit.write('merge 1:1 ADMITID using ".\Merge\Diagnosis.dta", keep(master match) nogenerate' + '\r\n')
    outpit.write('merge 1:1 ADMITID using ".\Merge\Diagnosis_Followup.dta", keep(master match) nogenerate' + '\r\n')
    outpit.write('merge 1:1 ADMITID using ".\Merge\Encounters.dta", keep(master match) nogenerate' + '\r\n')
    outpit.write('merge 1:1 ADMITID using ".\Merge\Pharmacy.dta", keep(master match) nogenerate' + '\r\n')
    outpit.write('merge 1:1 ADMITID using ".\Merge\Procedures.dta", keep(master match) nogenerate' + '\r\n')
    outpit.write('merge 1:1 ADMITID using ".\Merge\Readmit.dta", keep(master match) nogenerate' + '\r\n')
    for x in range(1, 10):
        outpit.write('capture replace FLWUP' + str(x) + 'CAT = "MAJOR BLEED" if FLWUP' + str(x) + 'CAT == "MINOR BLEED" & TRNSFUSNUM > 0' + '\r\n')
    outpit.write('compress' + '\r\n')
    outpit.write('save .\MasterMerge, replace' + '\r\n')


mainload()
