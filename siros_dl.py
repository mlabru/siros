# -*- coding: utf-8 -*-
"""
siros_dl
download e parser do arquivo csv de registros do SIROS

2022/fev  1.0  mlabru   initial version (Linux/Python)
"""
# < imports >--------------------------------------------------------------------------------------

# python library
import csv
import datetime
import io
import logging
import os
import pathlib
import sys
import requests
import zipfile

# .env
from dotenv import load_dotenv

# < defines >--------------------------------------------------------------------------------------

# logging level
DI_LOG_LEVEL = logging.INFO

# siros.anac.gov.br - registros
DS_REGS_URL = "https://siros.anac.gov.br/siros/registros/registros/serie/{}/"
DS_REGS_FN = "registros_{}.csv"

# siros.anac.gov.br - codeshares
DS_CDSH_URL = "https://siros.anac.gov.br/siros/registros/codeshare/serie/{}/"
DS_CDSH_FN = "codeshare_{}.zip"

# -------------------------------------------------------------------------------------------------
def download_registers(fs_url, fs_fname):
    """
    download SIROS registers

    :param fs_url (str): URL do SIROS
    :param fs_fname (str): CSV filename

    :return (lst): registers CSV list 
    """
    # logger
    logging.info(f"Downloading registers {fs_fname}")

    # registers file path
    ls_path = pathlib.Path(os.path.join("./registros", fs_fname))

    # file exists ?
    if ls_path.is_file():
        # logger
        logging.info("Reading from file...")

        # open local file 
        with open(ls_path, "r") as lfh:
            # return registers CSV list
            return [row for row in csv.reader(lfh.read().splitlines(), delimiter=';')]

    # logger
    logging.info("Downloading from site...")
 
    # request de dados
    l_response = requests.get(fs_url + fs_fname)

    # ok ?
    if 200 == l_response.status_code:
        # create registers file
        with open(ls_path, 'w') as lfh:
            # save registers
            lfh.write(l_response.text)

            # return registers CSV list
            return [row for row in csv.reader(l_response.text.splitlines(), delimiter=';')]
        
    # senão,...
    else:
        # logger
        logging.fatal("siros.anac.gov.br está inacessível. Aborting.")

        # quit with error
        sys.exit(255)

    # return error
    return None

# -------------------------------------------------------------------------------------------------
def download_codeshares(fs_url, fs_fname):
    """
    download SIROS codeshares

    :param fs_url (str): URL do SIROS
    :param fs_fname (str): CSV filename

    :return (lst): codeshares CSV list 
    """
    # logger
    logging.info(f"Downloading codeshares {fs_fname}")

    # codeshares file path
    ls_path = pathlib.Path(os.path.join("./registros", fs_fname))

    # file exists ?
    if ls_path.is_file():
        # logger
        logging.info("Reading from file...")

        # open zipfile 
        with zipfile.ZipFile(ls_path) as lzfh:
            # open csv file 
            with lzfh.open("codeshare.csv", 'r') as lfh:
                # return codeshares CSV list
                return [row for row in csv.reader(io.TextIOWrapper(lfh, "utf-8"), delimiter=';')]

    # logger
    logging.info("Downloading from site...")
 
    # request de dados
    l_response = requests.get(fs_url + fs_fname)

    # ok ?
    if 200 == l_response.status_code:
        # zipfile
        with open(ls_path, "wb") as lzfh:
            # save zip file
            lzfh.write(l_response.content)

        # open zipfile
        with zipfile.ZipFile(io.BytesIO(l_response.content)) as lzfh:
            # open csv file 
            with lzfh.open("codeshare.csv", 'r') as lfh:
                # return codeshares CSV list
                return [row for row in csv.reader(io.TextIOWrapper(lfh, "utf-8"), delimiter=';')]

    # senão,...
    else:
        # logger
        logging.fatal("siros.anac.gov.br está inacessível. Aborting.")

        # quit with error
        sys.exit(255)

    # return error
    return None

# -------------------------------------------------------------------------------------------------
def get_siros():
    """
    get list of SIROS RPLs for today
    """
    # creating the date object of today's date
    ldt_today = datetime.date.today()
    # ldt_today = datetime.datetime.strptime("2022-02-16", "%Y-%m-%d").date()

    # download codeshares from SIROS site
    llst_codeshares = download_codeshares(DS_CDSH_URL.format(ldt_today.year), DS_CDSH_FN.format(ldt_today))

    # parse codeshares
    ldct_codeshare = parse_codeshares(llst_codeshares, ldt_today)

    # download registers from SIROS site
    llst_regs = download_registers(DS_REGS_URL.format(ldt_today.year), DS_REGS_FN.format(ldt_today))

    # parse codeshares
    llst_regs = parse_registers(llst_regs, ldt_today)

    # merge codeshares
    llst_regs = merge_codeshare(llst_regs, ldct_codeshare)

    # return list of SIROS RPLs
    return llst_regs

# -------------------------------------------------------------------------------------------------
def merge_codeshare(fdt_today, fs_codeshare, fdct_rpls, ft_callsign):
    """
    trata codeshare
    AZU/5321 início: 25/08/2020 fim: 26/03/2022, AZU/5321 início: 04/01/2022 fim: 26/02/2022, AZU/5321 ...

    :param fdt_today (date): today's date
    :param fs_codeshare (str): codeshares
    :param fdct_rpls (dict): dicionário de RPL's
    :param ft_callsign (tuple): callsign    
    """
    # split codeshares
    llst_codeshares = fs_codeshare.split(',')

    # for all codeshares...
    for ls_codeshare in llst_codeshares:
        # split codeshare tokens
        llst_tokens = ls_codeshare.split()
 
        # split callsign
        llst_cs = llst_tokens[0].split('/')
        
        # callsign = (airliner, flight no)
        lt_callsign = (llst_cs[0], int(llst_cs[1]) if llst_cs[1].isdecimal() else llst_cs[1])

        # operational date ? 
        if "..." != llst_tokens[1]:  
            # data de início operação
            ldt_data_ini = datetime.datetime.strptime(llst_tokens[2], "%d/%m/%Y").date()
            # data de fim de operação
            ldt_data_fim = datetime.datetime.strptime(llst_tokens[4], "%d/%m/%Y").date()

            # à operar ?
            if not (ldt_data_ini <= fdt_today <= ldt_data_fim):
                # skip row
                continue

        # coloca na lista de RPLs
        fdct_rpls[lt_callsign] = fdct_rpls[ft_callsign]

# -------------------------------------------------------------------------------------------------
def parse_codeshares(flst_codeshares, fdt_today):
    """
    parse codeshares

    :param flst_codeshares (list): list of SIROS codeshares
    :param fdt_today (date): today's date
    """
    # logger
    logging.info("Parsing codeshares")

    # codeshares dictionary
    ldct_codeshare = {}

    # for all lines...
    for llst_row in flst_codeshares:
        # valid row ?
        if (len(llst_row) < 2) or ("Operadora" == llst_row[0]):
            # skip row
            continue

        # data de fim de operação
        ldt_data_fim = datetime.datetime.strptime(llst_row[6], "%d/%m/%Y").date()

        # à operar ?
        if not (fdt_today <= ldt_data_fim):
            # skip row
            continue

        # operator callsign = (airliner, flight no)
        lt_callsign_oper = (llst_row[0], int(llst_row[1]) if llst_row[1].isdecimal() else llst_row[1])

        # commercial. callsign = (airliner, flight no)
        lt_callsign_comm = (llst_row[2], int(llst_row[3]) if llst_row[3].isdecimal() else llst_row[3])

        # não está na lista de codeshares ?
        if not lt_callsign_oper in ldct_codeshare:
            # create codeshare
            ldct_codeshare[lt_callsign_oper] = [lt_callsign_comm]

        # senão, already exists
        elif lt_callsign_comm not in ldct_codeshare[lt_callsign_oper]:
            # append codeshare
            ldct_codeshare[lt_callsign_oper].append(lt_callsign_comm)

    # return codeshares dictionary
    return ldct_codeshare

# -------------------------------------------------------------------------------------------------
def parse_registers(flst_registers, fdt_today):
    """
    parse registers

    :param flst_registers (list): list of SIROS registers
    :param fdt_today (date): today's date
    """
    # logger
    logging.info("Parsing registers")

    # RPLs dictionary
    ldct_rpls = {}

    # for all lines...
    for llst_row in flst_registers:
        # valid row ?
        if (len(llst_row) < 2) or ("Cód. Empresa" == llst_row[0]):
            # skip row
            continue

        # data de início operação
        ldt_data_ini = datetime.datetime.strptime(llst_row[15], "%Y-%m-%d").date()
        # data de fim de operação
        ldt_data_fim = datetime.datetime.strptime(llst_row[16], "%Y-%m-%d").date()

        # à operar ?
        if not (ldt_data_ini <= fdt_today <= ldt_data_fim):
            # skip row
            continue

        # lista de vôos semanais
        llst_weekday = llst_row[4:11]

        # not flying today ?
        if 0 == int(llst_weekday[fdt_today.weekday()]):
            # skip row
            continue

        # flight number is all numbers ?
        if llst_row[2].isdecimal():
            # callsign = (airliner, flight no)
            lt_callsign = (llst_row[0], int(llst_row[2]))

        else:             
            # callsign = (airliner, flight no)
            lt_callsign = (llst_row[0], llst_row[2])

        # não está na lista de RPLs ?
        if not lt_callsign in ldct_rpls:
            # partida do vôo
            ls_hour, ls_min = llst_row[23].split(':')
            ldt_partida = datetime.datetime.combine(fdt_today, 
                                                    datetime.time(hour=int(ls_hour), 
                                                                  minute=int(ls_min), 
                                                                  tzinfo=datetime.timezone.utc))
            # chegada do vôo
            ls_hour, ls_min = llst_row[24].split(':')
            ldt_chegada = datetime.datetime.combine(fdt_today, 
                                                    datetime.time(hour=int(ls_hour), 
                                                                  minute=int(ls_min), 
                                                                  tzinfo=datetime.timezone.utc))
            # chegada no dia seguinte ?
            if ldt_partida > ldt_chegada:
                # acrescenta 1 dia 
                ldt_chegada += datetime.timedelta(days=1)

            # coloca na lista de RPLs
            ldct_rpls[lt_callsign] = {"partida": int(ldt_partida.timestamp() * 1000),   # partida
                                      "chegada": int(ldt_chegada.timestamp() * 1000)}   # chegada
            # logging.debug("ldct_rpls: %s", str(ldct_rpls[lt_callsign]))

    # return RPLs dictionary
    return ldct_rpls
    
# -------------------------------------------------------------------------------------------------
def main():
    """
    drive application
    """
    # get SIROS
    get_siros()
    
# -------------------------------------------------------------------------------------------------
# this is the bootstrap process

if "__main__" == __name__:
    # logger
    logging.basicConfig(level=DI_LOG_LEVEL)

    # disable logging
    # logging.disable(sys.maxsize)
    
    # run application
    sys.exit(main())
    
# < the end >--------------------------------------------------------------------------------------
