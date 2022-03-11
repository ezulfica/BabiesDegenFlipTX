import numpy as np
import pandas as pd
import requests
import time
from alive_progress import alive_bar
from os.path import exists

pd.set_option('display.float_format', lambda x: '%.8f' % x)

class BabiesDegenFlipTx :

    """Class of function used to pull data from elrond API"""

    def __init__(self, date_to, date_from):
        self.wallet = ["erd1qqqqqqqqqqqqqpgqvza50nx0pvr6mkylt7n62wt77hzt5z9a7m2qg84rws",
                       "erd1qqqqqqqqqqqqqpgqgsuezj5g342sk4gy634pnv6v50tucnts7m2qevc5hc"]
        self.DAO_Wallet = "erd1wtwuwretnf3xlvklfs8s0ljv30u0f2nmtwpc47emdhv4s923k00s353qsu"
        self.WoL = True
        self.date_from = pd.Timestamp(date_from).timestamp()
        self.date_to = pd.Timestamp(date_to).timestamp()

    def get_wallet_tx(self, wallet):
        """Get all transaction from a wallet, between the defined date"""

        params = {
            "status": "success",  # succes, pending,
            "before": int(self.date_to),
            "after": int(self.date_from),
        }

        URL = f"https://api.elrond.com/accounts/{wallet}/transactions/"

        size = requests.get(URL + "count", params).json()

        params["size"] = (size > 10000) * 10000 + size * (size <= 10000)

        gcd = size // 10000 + 1
        if gcd > 1 :
            all_tx = pd.DataFrame()
            for j in range(gcd):

                got_tx = False
                while got_tx == False :
                    try :
                        tx_inf = pd.DataFrame(requests.get(URL, params).json())
                        got_tx = True
                    except :
                        time.sleep(2)
                        got_tx = False

                date_to = tx_inf["timestamp"][len(tx_inf) - 1]
                params["before"] = date_to
                all_tx = pd.concat([all_tx, tx_inf], axis=0)
            all_tx = all_tx.drop_duplicates(subset="txHash", keep=False)

        else:
            all_tx = pd.DataFrame(requests.get(URL, params).json())

        #Keep only the tx where the flip smart contract applies
        if len(all_tx) >= 1 :
            all_tx = all_tx[all_tx["action"] == {'category': 'scCall', 'name': 'play'}]
            all_tx = all_tx[["txHash", "sender", "value", "receiver", "timestamp"]]
            all_tx["value"] = all_tx["value"].astype("float")
            all_tx["value"] = all_tx["value"] / 10 ** 18
            all_tx["timestamp"] = pd.to_datetime(all_tx["timestamp"], unit='s')
            all_tx["fees"] = all_tx["value"] * 0.05

        return(all_tx)

    def get_all_tx(self):
        dt_tx = pd.DataFrame()
        for wallet in self.wallet :
            dt = self.get_wallet_tx(wallet)
            dt_tx = pd.concat([dt, dt_tx], axis = 0)

        dt_tx.sort_values(by="timestamp", ascending=False)
        self.wallet_tx = dt_tx
        print("Transactions scrapped !")

    def get_sc_results_from_tx(self) :
        dt_tx = pd.DataFrame()
        for tx in self.wallet_tx["txHash"] :
            got_sc = False
            while got_sc == False :
                try:
                    req_tx = requests.get("https://api.elrond.com/transactions/" + tx).json()["results"]
                    got_sc = True
                except:
                    time.sleep(2)
                    got_sc = False

            tx_result = pd.DataFrame(req_tx)

            tx_result.drop(columns=["nonce", "gasLimit", "gasPrice", "data",
                                    "callType", "miniBlockHash"],
                           inplace=True)

            tx_result.rename(columns={"hash": "txHash"}, inplace=True)
            tx_result["status"] = (len(tx_result) == 3) * True + (len(tx_result) == 2) * False

            #At the end of every transaction, we append the row collected
            dt_tx = pd.concat([dt_tx, tx_result], axis=0)

        win_lose = dt_tx[["originalTxHash", "status"]].\
            drop_duplicates(subset = "originalTxHash").\
            rename(columns={"originalTxHash" : "txHash"})

        dt_tx.drop(columns="status", inplace = True)

        dt_tx = pd.concat([all_tx, dt_tx], axis = 0)
        dt_tx["prevTxHash"] = dt_tx["prevTxHash"].fillna(dt_tx["txHash"])
        dt_tx["originalTxHash"] = dt_tx["originalTxHash"].fillna(dt_tx["txHash"])
        dt_tx = dt_tx.merge(win_lose, on = "txHash")

        self.wallet_tx = dt_tx
        self.WoL = False
        print("Smart contract results scrapped !")

    def get_wallet_WoL(self):

        """Get smart contract results from a transaction hash
        2 results mean the money isn't going back to the original sender (the player) meaning it's a lose
        3 means it's a win. """
        if self.WoL :
            URL = f"https://api.elrond.com/transactions/"
            win_lose = []
            with alive_bar(len(self.wallet_tx), force_tty=True) as bar :
                for txhash in self.wallet_tx["txHash"] :
                    got_sc = False
                    while got_sc == False:
                        try:
                            results = len(requests.get(URL + txhash).json()["results"])
                            got_sc = True
                        except:
                            time.sleep(0.5)
                            got_sc = False

                    win_lose.append((results == 2) * False + (results == 3) * True)
                    bar()

            win_lose = np.array(win_lose)
            self.wallet_tx["status"] = np.uint8(win_lose)
            self.wallet_tx["balance"] = (self.wallet_tx["status"] == 0) * (- self.wallet_tx["value"]) + \
                                        (self.wallet_tx["status"] == 1) * (self.wallet_tx["value"]) * 0.9

            print("Win/Loose status added !")

    def get_winstreak(self):
        players = self.wallet_tx[~self.wallet_tx["sender"].isin(self.wallet)]["sender"].unique()
        streak = []
        for player in players :
            win_status = self.wallet_tx[self.wallet_tx["sender"] == player]["status"]
            win_streak = (win_status != win_status.shift()).cumsum()
            win_streak = np.max(win_status.groupby(win_streak).cumsum())
            streak.append(win_streak)

        player_win = pd.DataFrame({"sender" : players, "win_streak" : streak})
        self.wallet_tx = self.wallet_tx.merge(player_win, on = "sender")
        print("Longest win-streak per player added to database !")


    def export_data(self, name) :
        self.wallet_tx.sort_values(by="timestamp", ascending=False, inplace=True)
        self.wallet_tx.to_json(name, orient = "split", indent = 1, date_format='iso')
        print(".json database saved !")

    def update_data(self, name):
        if exists(name) :
            dt_tx = pd.read_json(name, orient = "split")
            self.wallet_tx = pd.concat([dt_tx, self.wallet_tx], axis = 0)
            self.wallet_tx = self.wallet_tx.drop_duplicates("txHash")
            self.wallet_tx.drop(columns= ["win_streak"], inplace = True)
            self.wallet_tx.sort_values(by = "timestamp", ascending = False, inplace = True)
            self.get_winstreak()
            self.export_data(name)
        else :
            print(".json file not found, creating a new one")
            self.export_data(name)
