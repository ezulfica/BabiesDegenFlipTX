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

    def get_tx_from_wallet(self):
        """Get the last 10 000 transactions from a wallet trough elrond api"""

        URL = f"https://api.elrond.com/accounts/{self.wallet}/transactions/"
        size = requests.get(URL + "count", {"size": 10000, "status" : "success"}).json()

        if size > 10000 :
            size = 10000
            print("There is too much transaction counted, limited to the last 10 000 .")

        params = {
            "size" : size,
            "status": "success" #succes, pending,
        }

        all_tx = pd.DataFrame(requests.get(URL, params).json())["txHash"]
        self.wallet_tx = all_tx
        return self.wallet_tx

    def get_tx_details(txhash) :
        URL = "https://api.elrond.com/transactions/"
        params = {
            "txHash" : txhash
        }
        return requests.get(URL + txhash)

    def get_tx_details_df(wallet_tx) :
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
            dt_tx = dt_tx[dt_tx["timestamp"] >= pd.to_datetime("2022-02-26 19:00:00")] #Remove all tx before the official release of the game

        dt_tx["value"] = dt_tx["value"].astype(np.int64) / 10**18 #to get the correct amount of egld
        dt_tx["timestamp"] = pd.to_datetime(dt_tx["timestamp"], unit='s') #to change the unix timestamp into UTC timestamp
        return dt_tx

    def get_number_request_failed(self, dt_tx,) :
        """return the number of request failed"""
        return len(self.wallet_tx) - dt_tx["originalTxHash"].nunique() - 1



