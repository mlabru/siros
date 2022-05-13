# -*- coding: utf-8 -*-
"""
siros_mon

2022.fev  mlabru  initial version (Linux/Python)
"""
# < imports >----------------------------------------------------------------------------------

# python library
import json
import logging
import os
import sys

# import stomp
import stomper

# websocket
import websocket

# .env
from dotenv import load_dotenv

# local
import siros_dl as sdl
import sm_data as gd

# < defines >----------------------------------------------------------------------------------

# logging level
DI_LOG_LEVEL = logging.DEBUG

# ---------------------------------------------------------------------------------------------
def check_rpl(ft_callsign: tuple, fdct_flight: dict):
    """
    callback

    :param ft_callsign (tuple): callsign
    :param fi_timestamp (int): timestamp da detecção
    """
    # timestamp
    li_timestamp = fdct_flight.get("time", sys.maxsize)

    # first sight ?
    if ft_callsign not in gd.DDCT_FLIGHT_RPLS:
        # init SSR code
        li_code = -1

        # ssr dict: 'ssr': {'registration': 'PTGMU', 'transponder': {'code': 1839}}
        ldct_ssr = fdct_flight.get("ssr", {})

        if ldct_ssr:
            # transponder
            ldct_trp = ldct_ssr.get("transponder", {})

            if ldct_trp:
                # SSR code
                li_code = int(ldct_trp.get("code", -1))

        # time diff (adiantado < 0, on-time = 0, atrasado > 0)
        li_diff = li_timestamp - gd.DDCT_SIROS_RPLS[ft_callsign]["partida"]

        # init flight dict
        gd.DDCT_FLIGHT_RPLS[ft_callsign] = {"code": li_code,
                                              "first": li_timestamp,
                                              "last": li_timestamp,
                                              "diff": li_diff}
        # logger
        logging.debug("first sight of %s: %s from: %s [%s]",
                      str(ft_callsign),
                      str(gd.DDCT_FLIGHT_RPLS[ft_callsign]),
                      str(gd.DDCT_SIROS_RPLS[ft_callsign]),
                      str((li_diff // 3600000, (li_diff // 60000) % 60)))
    # senão,...
    else:
        # update timestamp
        gd.DDCT_FLIGHT_RPLS[ft_callsign]["last"] = li_timestamp

# ---------------------------------------------------------------------------------------------
def on_closed(f_ws, f_p2, f_p3):
    """
    callback
    """
    # logger
    logging.info("# Closed # (%s, %s, %s)", str(f_ws), str(f_p2), str(f_p3))

# ---------------------------------------------------------------------------------------------
def on_error(f_ws, fs_err):
    """
    callback
    """
    # logger
    logging.error(str(fs_err))

    # close WS
    f_ws.close()

# ---------------------------------------------------------------------------------------------
# pylint: disable=unused-argument
def on_msg(f_ws, fs_msg):
    """
    callback
    """
    # remove header
    li_ind = fs_msg.find('{')

    # decode message
    llst_message = json.loads(fs_msg[li_ind - 1: -1])["newPaths"]

    # scan message
    scan_msg(llst_message)

# ---------------------------------------------------------------------------------------------
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

# ---------------------------------------------------------------------------------------------
def scan_msg(flst_msg: list):
    """
    callback
    """
    # for all flights...
    for ldct_flight in flst_msg:
        # logger
        # logging.debug("ldct_flight: %s", str(ldct_flight))

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

        # flight number is all numbers ?
        if ls_reg[3:].isdecimal():
            # callsign = (airliner, flight no)
            lt_callsign = (ls_reg[:3], int(ls_reg[3:]))

        else:
            # callsign = (airliner, flight no)
            lt_callsign = (ls_reg[:3], ls_reg[3:])

        # flight in set ?
        if lt_callsign in gd.DDCT_SIROS_RPLS:
            # check for RPLs
            check_rpl(lt_callsign, ldct_flight)

# ---------------------------------------------------------------------------------------------
def main():
    """
    drive app
    """
    # take environment variables from .env.
    load_dotenv()

    # download e parser do arquivo csv de registros do siros
    gd.DDCT_SIROS_RPLS = sdl.get_siros()
    # logging.debug("gd.DDCT_SIROS_RPLS: %s", str(gd.DDCT_SIROS_RPLS))

    # dis/enable websocket trace
    # websocket.enableTrace(True)

    # create websocket
    gd.D_WSAPP = websocket.WebSocketApp(os.getenv("WS_URI"),
                                          header={"destination:/atc_topic/tracks",
                                                  "subscription:/atc_topic/tracks"},
                                          on_message=on_msg,
                                          on_error=on_error,
                                          on_close=on_closed)
    assert gd.D_WSAPP

    # set websocket's open callback
    gd.D_WSAPP.on_open = on_open

    # create DB connection (sqlite)
    # gd.D_CONN = sqlite3.connect("odin.db")
    # assert gd.D_CONN

    # logger
    logging.info("RUNNING....")

    # loop app
    gd.D_WSAPP.run_forever()

# ---------------------------------------------------------------------------------------------
# this is the bootstrap process

if "__main__" == __name__:

    # logger
    logging.basicConfig(level=DI_LOG_LEVEL)

    # disable logging
    # logging.disable(sys.maxint)

    # run application
    main()

# < the end >----------------------------------------------------------------------------------
