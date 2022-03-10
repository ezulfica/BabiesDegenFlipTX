import numpy as np
import pandas as pd
import requests
import time

pd.set_option('display.float_format', lambda x: '%.8f' % x)
class BabiesDegenFlipTx :

    """Class of function used to pull data from elrond API"""

    def __init__(self, wallet, date_to, date_from):
        self.wallet = wallet
        self.DAO_Wallet = "erd1wtwuwretnf3xlvklfs8s0ljv30u0f2nmtwpc47emdhv4s923k00s353qsu"
        self.WoL = True
        self.date_from = pd.Timestamp(date_from).timestamp()
        self.date_to = pd.Timestamp(date_to).timestamp()

    def get_tx_from_wallet(self, every_tx = False):
        """Get the last 10 000 transactions or every hash/info from a wallet trough elrond api between two dates"""

        params = {
            "status": "success",  # succes, pending,
            "before": int(self.date_to),
            "after": int(self.date_from),
        }

        URL = f"https://api.elrond.com/accounts/{self.wallet}/transactions/"

        size = requests.get(URL + "count", params).json()
        params["size"] = (size > 10000) * 10000 + size * (size <= 10000)

        if every_tx:
            gcd = size // 10000 + 1  # +1 to make sure I get all tx even if duplicates
            all_tx = pd.DataFrame()
            for j in range(gcd):
                tx_inf = pd.DataFrame(requests.get(URL, params).json())
                time.sleep(2)
                date_to = tx_inf["timestamp"][len(tx_inf) - 1]
                params["before"] = date_to
                all_tx = pd.concat([all_tx, tx_inf], axis=0)
            all_tx = all_tx.drop_duplicates(subset="txHash", keep=False)

        else:
            all_tx = pd.DataFrame(requests.get(URL, params).json())

        #Keep only the tx where the flip smart contract applies
        all_tx = all_tx[all_tx["action"] == {'category': 'scCall', 'name': 'play'}]
        #Remove the row when DAO Wallet is the sender (assuming these transactions are to test the smart contract)
        #all_tx = all_tx[all_tx["sender"] != self.DAO_Wallet]
        all_tx = all_tx[["txHash", "sender", "value", "receiver", "timestamp"]]
        self.wallet_tx = all_tx
        print("Transaction scrapped !")

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

            win_lose = np.array(win_lose)
            self.wallet_tx["status"] = win_lose
            print("Win/Loose status added !")

    def get_winstreak(self):
        players = self.wallet_tx[self.wallet_tx["sender"] != self.wallet]
        streak = []
        for player in players :
            #win_status = self.wallet_tx[self.wallet_tx["sender"] == player]["status"]
            win_status = all_tx[all_tx["sender"] == player]["status"]
            win_streak = (win_status != win_status.shift()).cumsum()
            win_streak = np.max(win_status.groupby(win_streak).cumsum())
            streak.append(win_streak)

        player_win = pd.DataFrame({"sender" : players, "win_streak" : streak})
        self.wallet_tx = self.wallet_tx.merge(player_win, on = "sender")

        print("Longest win-streak per player added to database !")


    def export_data(self, name):
        self.wallet_tx["value"] = self.wallet_tx["value"].astype(np.int64) / 10**18 #to get the correct amount of egld
        self.wallet_tx["timestamp"] = pd.to_datetime(self.wallet_tx["timestamp"], unit='s') #to change the unix timestamp into UTC timestamp
        self.vallet_tx["fees"] = self.wallet_tx["value"].astype(np.int64) * 0.05
        self.wallet_tx.to_json(name)
        print(".json database saved !")

    def update_data(self, name):
        try :
            dt_tx = pd.read_json(name)
            self.wallet_tx = pd.concat([dt_tx, self.wallet_tx], axis = 0)
            self.wallet_tx = self.wallet_tx.drop_duplicates("txHash", keep = False)
            self.wallet_tx.drop("win_streak", inplace = True)
            self.get_winstreak()
            self.export_data(name)

        except :
            print(".json file not found, creating a new one")
            self.export_data(name)
