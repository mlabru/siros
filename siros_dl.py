# -*- coding: utf-8 -*-
"""
get_regs
download e parser do arquivo csv de registros do sirus

2022/fev  1.0  mlabru   initial version (Linux/Python)
"""
# < imports >--------------------------------------------------------------------------------------

# python library
import csv
import datetime
import logging
import os
import pathlib
import sys
import requests

# .env
from dotenv import load_dotenv

# < defines >--------------------------------------------------------------------------------------

# logging level
DI_LOG_LEVEL = logging.INFO

# siros.anac.gov.br - registros
DS_REGS_URL = "https://siros.anac.gov.br/siros/registros/registros/serie/{}/"
DS_REGS_FN = "registros_{}.csv"

# -------------------------------------------------------------------------------------------------
def download_registers(fs_url, fs_fname):
    """
    download SIROS registers
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
            # return read file
            return lfh.read()

    # logger
    logging.info("Downloading from site...")
 
    # request de dados
    l_response = requests.get(fs_url + fs_fname)

    # ok ?
    if 200 == l_response.status_code:
        # create registers file
        with open(ls_path, "w") as lfh:
            # save registers
            lfh.write(l_response.text)
        
    # senão,...
    else:
        # logger
        logging.fatal("siros.anac.gov.br está inacessível. Aborting.")

        # quit with error
        sys.exit(255)

    # return registers
    return l_response.text

# -------------------------------------------------------------------------------------------------
def get_siros():
    """
    get list of siros RPLs for today
    """
    # creating the date object of today's date
    ldt_today = datetime.date.today()
    # ldt_today = datetime.datetime.strptime("2022-02-06", "%Y-%m-%d").date()

    # download registers from SIROS site
    l_regs = download_registers(DS_REGS_URL.format(ldt_today.year), DS_REGS_FN.format(ldt_today))

    # return
    return parse_registers(l_regs, ldt_today)

# -------------------------------------------------------------------------------------------------
def parse_registers(fs_registers, fdt_today):
    """
    parse registers
    """
    # logger
    logging.info("Parsing registers")

    # RPLs dictionary
    ldct_rpls = {}

    # create CSV reader
    l_reader = csv.reader(fs_registers.splitlines(), delimiter=';')

    # for all lines...
    for llst_row in l_reader:
        # valid row ?
        if (len(llst_row) < 2) or ("Cód. Empresa" == llst_row[0]):
            # skip row
            continue

        # data de início operação
        ldt_data_ini = datetime.datetime.strptime(llst_row[15], "%Y-%m-%d").date()
        # data de fim de operação
        ldt_data_fim = datetime.datetime.strptime(llst_row[16], "%Y-%m-%d").date()

        # today is not between start and end dates ?
        if not (ldt_data_ini <= fdt_today <= ldt_data_fim):
            # skip row
            continue

        # lista de vôos semanais
        llst_weekday = llst_row[4:11]

        # not flying today ?
        if 0 == int(llst_weekday[fdt_today.weekday()]):
            # skip row
            continue

        # callsign = airliner + flight no
        ls_callsign = llst_row[0] + llst_row[2]

        # está na lista de RPLs ?
        if not ls_callsign in ldct_rpls:
            # coloca na lista de RPLs
            ldct_rpls[ls_callsign] = {"origem": llst_row[19],     # origem do vôo
                                      "destino": llst_row[21],    # destino do vôo
                                      "partida": llst_row[23],    # partida do vôo
                                      "chegada": llst_row[24]}    # chegada do vôo

    # return
    return ldct_rpls
    
# -------------------------------------------------------------------------------------------------
def main():
    """
    drive application
    """
    # get siros
    get_siros()
    
# -------------------------------------------------------------------------------------------------
# this is the bootstrap process

if "__main__" == __name__:
    # logger
    logging.basicConfig(level=DI_LOG_LEVEL)

    # disable logging
    # logging.disable(sys.maxint)
    
    # run application
    sys.exit(main())
    
# < the end >--------------------------------------------------------------------------------------
