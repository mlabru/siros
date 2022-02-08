# -*- coding: utf-8 -*-
"""
siros_mon

2022/fev  1.0  mlabru  initial version (Linux/Python)
"""
# < imports >--------------------------------------------------------------------------------------

# python library
import json
import logging
import os
import sys

import websocket

# .env
from dotenv import load_dotenv

# import stomp
import stomper

# SQLite
import sqlite3

# local
import sm_data as gdat
import siros_dl as sdl

# < defines >--------------------------------------------------------------------------------------

# logging level
DI_LOG_LEVEL = logging.INFO

# -------------------------------------------------------------------------------------------------
def check_rpl(flst_msg):
    """
    callback
    """
    # for all flights...
    for ldct_flight in flst_msg:
        # ssr dict
        ldct_ssr = ldct_flight.get("ssr", {})   

        if not ldct_ssr:
            # skip
            continue

        # registration
        ls_reg = ldct_ssr.get("registration", None)

        if not ls_reg:
            # skip
            continue

        # flight in set ?
        if ls_reg in gdat.DSET_SIROS_RPLS: 
            # save on flight set
            gdat.DSET_FLIGHT_RPLS.add(ls_reg)

            # logger
            print("FLIGHT_RPLS", gdat.DSET_FLIGHT_RPLS)

# -------------------------------------------------------------------------------------------------
def on_closed(f_ws, p2, p3):
    """
    callback
    """
    # logger
    logging.info("# Closed # (%s, %s, %s)", str(f_ws), str(p2), str(p3))

# -------------------------------------------------------------------------------------------------
def on_error(f_ws, fs_err):
    """
    callback
    """
    # logger
    logging.error(str(fs_err))

    # close WS
    f_ws.close()

# -------------------------------------------------------------------------------------------------
def on_msg(f_ws, fs_msg): 
    """
    callback
    """
    # remove header
    li_ind = fs_msg.find('{')    

    # decode message
    llst_message = json.loads(fs_msg[li_ind - 1 : -1])["newPaths"]
    # logging.debug("message: %s", str(llst_message))
        
    # check for RPLs
    check_rpl(llst_message)
    
# -------------------------------------------------------------------------------------------------
def on_open(f_ws):
    """
    callback
    """
    # subscribe
    l_sub = stomper.subscribe("/atc_topic/tracks", "", ack="auto")

    # send
    f_ws.send(l_sub)

    # logger
    logging.info("OPENING....")

# -------------------------------------------------------------------------------------------------
def main():
    """
    drive app
    """
    # take environment variables from .env.
    load_dotenv()

    # download e parser do arquivo csv de registros do siros
    gdat.DSET_SIROS_RPLS = sdl.get_siros()

    # dis/enable websocket trace
    # websocket.enableTrace(True)

    # create websocket
    gdat.D_WSAPP = websocket.WebSocketApp(os.getenv("WS_URI"),
                                          header={"destination:/atc_topic/tracks",
                                                  "subscription:/atc_topic/tracks"},
                                          on_message=on_msg,
                                          on_error=on_error,
                                          on_close=on_closed)
    assert gdat.D_WSAPP
    
    # set websocket's open callback
    gdat.D_WSAPP.on_open = on_open

    # create DB connection (sqlite)
    gdat.D_CONN = sqlite3.connect("odin.db")
    assert gdat.D_CONN

    # logger
    logging.info("RUNNING....")

    # loop app
    gdat.D_WSAPP.run_forever()

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
