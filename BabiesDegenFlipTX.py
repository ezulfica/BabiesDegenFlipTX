import numpy as np
import pandas as pd
import requests
import time

pd.set_option('display.float_format', lambda x: '%.8f' % x)

DAO_Wallet = "erd1wtwuwretnf3xlvklfs8s0ljv30u0f2nmtwpc47emdhv4s923k00s353qsu" #BabiesDegen DAO Wallet
FLIP_Wallet = "erd1qqqqqqqqqqqqqpgqvza50nx0pvr6mkylt7n62wt77hzt5z9a7m2qg84rws" #BabiesDegenFlip Wallet

class BabiesDegenFlipTx :

    """Class of function used to pull data from elrond API"""

    def __init__(self, wallet):
        self.wallet = wallet

    def get_tx_from_wallet(self, date_from = "2022-02-26 19:00:00", date_to = pd.to_datetime("today"), only_tx_hash = True, every_tx = False):
        """Get the last 10 000 transactions hash/info from a wallet trough elrond api between two dates"""

        size = self.get_number_of_tx_by_wallet(self.wallet ,date_from, date_to)
        date_from = pd.Timestamp(date_from).timestamp()
        date_to = pd.Timestamp(date_to).timestamp()

        URL = f"https://api.elrond.com/accounts/{self.wallet}/transactions/"

        params = {
            "status": "success",  # succes, pending,
            "before": int(date_to),
            "after": int(date_from),
        }

        params["size"] = (size > 10000) * 10000 + size * (size <= 10000)

        if every_tx :
            all_tx = pd.DataFrame()
            gcd = size // 10000 + 2 #+2 to make sure I get all tx even if duplicates
            for j in gcd:
                tx_inf = pd.DataFrame(requests.get(URL, params).json())
                date_to = tx_inf["timestamp"][len(tx_inf)]
                params["before"] = date_to
                all_tx = pd.concat([all_tx, tx_inf], axis=0)
            all_tx = all_tx.drop_duplicates()

        else :
            all_tx = pd.DataFrame(requests.get(URL, params).json())

        self.wallet_tx = all_tx
        if only_tx_hash :
            return(all_tx["txHash"])


    def get_tx_details_df(self, wallet_tx) :
        dt_tx = pd.DataFrame()
        details_needed = ["txHash", "miniBlockHash", 'receiver', 'sender', 'value', "timestamp", 'price']

        for tx in wallet_tx:
            #If there is an error for a request, we skip it and wait 10 sec before resuming the pull of data
            try:
                req_tx = get_tx_details(tx).json()
            except:
                time.sleep(10)
                continue

            #Not the optimal way, but the easiest to convert the dictionnary into a dataframe and then aggregate everything
            tx_primary = {}
            for details in details_needed:
                tx_primary[details] = req_tx[details]
            tx_primary = pd.DataFrame(tx_primary, index=[0])


            #If there is no smart contract results, we skip the merging part with the primary transaction
            try:
                tx_result = pd.DataFrame(req_tx["results"])
                tx_result.drop(columns=["nonce", "gasLimit", "gasPrice", "data", "callType"], inplace=True)
                tx_result.rename(columns={"hash": "txHash"}, inplace=True)
                tx_result['price'] = tx_primary["price"][0]
                tx_info = tx_primary.merge(tx_result, how="outer")
            except:
                print("no sc_result from tx")
                continue

            #At the end of every transaction, we append the row collected
            dt_tx = pd.concat([dt_tx, tx_info], axis=0)

        dt_tx["value"] = dt_tx["value"].astype(np.int64) / 10**18 #to get the correct amount of egld
        dt_tx["timestamp"] = pd.to_datetime(dt_tx["timestamp"], unit='s') #to change the unix timestamp into UTC timestamp

        dt_tx["prevTxHash"]  = dt_tx["prevTxHash"].fillna(dt_tx["txHash"])
        dt_tx["originalTxHash"] =  dt_tx["originalTxHash"].fillna(dt_tx["txHash"])
        return dt_tx

    def get_number_of_tx_by_wallet(self, wallet,date_from, date_to):
        date_from = pd.Timestamp(date_from).timestamp()
        date_to = pd.Timestamp(date_to).timestamp()

        URL = f"https://api.elrond.com/accounts/{wallet}/transactions/"

        params = {
            "status": "success", #succes, pending,
            "before" : int(date_to),
            "after" : int(date_from)
        }
        return requests.get(URL + "count", params).json()

    def get_tx_details(self, txhash) :
        URL = "https://api.elrond.com/transactions/"
        return requests.get(URL + txhash)

    def get_only_play_tx(self):
        play_tx = wallet_tx[wallet_tx["action"] == {'category': 'scCall', 'name': 'play'}]["txHash"]
        self.play_tx = play_tx

    def get_number_request_failed(self, dt_tx, wallet_tx) :
        """return the number of request failed"""
        return len(wallet_tx) - dt_tx["originalTxHash"].nunique()



