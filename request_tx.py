import numpy as np
import pandas as pd
import requests
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import concurrent
from tqdm import tqdm
from os.path import exists

pd.set_option('display.float_format', lambda x: '%.8f' % x)

class BabiesDegenFlipTx:

    """Class of function used to pull data from elrond API"""

    def __init__(self, date_to, date_from):

        self.wallet = ["erd1qqqqqqqqqqqqqpgqvza50nx0pvr6mkylt7n62wt77hzt5z9a7m2qg84rws", #First flip wallet adress
                       "erd1qqqqqqqqqqqqqpgqgsuezj5g342sk4gy634pnv6v50tucnts7m2qevc5hc"] #New flip wallet adress
        self.DAO_Wallet = "erd1wtwuwretnf3xlvklfs8s0ljv30u0f2nmtwpc47emdhv4s923k00s353qsu" #DAO wallet adress
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

        size = requests.get(URL + "count", params).json() #count the number of tx in the wallet

        # if the number exceed 10k, we downsizing it to 10k because it's the limit
        params["size"] = (size > 10000) * 10000 + size * (size <= 10000)

        gcd = size // 10000 + 1 #calculating the greatest common divisor to get the right number of loop
        if gcd > 1 :
            all_tx = pd.DataFrame()

            #To prevent requests failure, we are looping until it's good, with 0.5 sec between each request
            #We are starting by taking the last 10 000 transactions from the most recent date
            #then we restart by taking the new recent (the older of the last request).
            for _ in range(gcd):
                got_tx = False
                while got_tx == False :
                    try :
                        tx_inf = pd.DataFrame(requests.get(URL, params).json())
                        got_tx = True
                    except :
                        time.sleep(0.5)

                date_to = tx_inf["timestamp"][len(tx_inf) - 1] #Taking the oldest date
                params["before"] = date_to #updating the request parameters for the new iteration
                all_tx = pd.concat([all_tx, tx_inf], axis=0) #concatenating the data into one unique dataframe
            all_tx = all_tx.drop_duplicates(subset="txHash", keep=False) #removing duplicates

        else:
            all_tx = pd.DataFrame(requests.get(URL, params).json())

        #Keep only the tx where the flip smart contract applies
        if len(all_tx) >= 1 :
            all_tx = all_tx[all_tx["action"] == {'category': 'scCall', 'name': 'play'}] #keeping only the transaction linked to the flip game
            all_tx = all_tx[["txHash", "sender", "value", "receiver", "timestamp"]] #keeping the following columns
            all_tx["value"] = all_tx["value"].astype("float") #settings value as a number (it's EGLD values)
            all_tx["value"] = all_tx["value"] / 10 ** 18 #diving it to get the right value
            all_tx["timestamp"] = pd.to_datetime(all_tx["timestamp"], unit='s') #convert unix timestamp to an easier date format
            all_tx["fees"] = all_tx["value"] * 0.05 #getting the 5% fees

        return(all_tx)

    def get_all_tx(self):

        """
        Get all transactions for every wallet we want to look into it
        Here's it's the two flip wallet
        """

        dt_tx = pd.DataFrame()
        for wallet in self.wallet :
            dt = self.get_wallet_tx(wallet)
            dt_tx = pd.concat([dt, dt_tx], axis = 0)

        dt_tx.sort_values(by="timestamp", ascending=False)
        self.wallet_tx = dt_tx
        print("Transactions scrapped !")

    def get_wallet_WoL(self, thread = 100):

        """
        Get smart contract results from a transaction hash then deciding if the tx is a win or a lose
        according to the number of results.
        2 = lose, 3 = win (for the player)
        """

        URL = f"https://api.elrond.com/transactions/"

        len_wallet = len(self.wallet_tx)

        #Using multithread to fasten api requests
        #tqdm --> progressing bar that we increment every successful request
        session = requests.Session()
        with tqdm(total = len_wallet) as pbar:
            with ThreadPoolExecutor(max_workers = thread) as executor:
                win_lose = [executor.submit(self.multithread_fetch_wl, session, url=URL + txhash) for txhash in self.wallet_tx["txHash"]]
                for _ in as_completed(win_lose) :
                    pbar.update(1)

        win_lose = np.array([wl.result() for wl in win_lose])

        self.wallet_tx["status"] = (win_lose == 2) * False + (win_lose == 3) * True

        #The balance of player, if every tx is a win, we multiply by 0.9 (what's gained) or substract by the value bet
        self.wallet_tx["balance"] = (self.wallet_tx["status"] == 0) * (- self.wallet_tx["value"] ) + \
                                    (self.wallet_tx["status"] == 1) * (self.wallet_tx["value"]) * 0.9

        print("Win/Loose status added !")

    def multithread_fetch_wl(self, session, url):
        """
        Function used to for the multi threading request to get transactions win or lose
        After getting number of a smart contract result (2 or 3) we return a boolean as win/lose
        """

        get_sc = False
        while get_sc == False :
            try :
                wl = len(session.get(url=url).json()["results"])
                get_sc = True
            except :
                get_sc = False

        return wl

    def get_winstreak(self):

        """
        Function to get the longest winstreak per player
        """

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
        """
        Export data from BabiesDegenFlip transactions to a json file
        """
        self.wallet_tx.sort_values(by="timestamp", ascending=False, inplace=True)
        self.wallet_tx["timestamp"] = self.wallet_tx["timestamp"].astype(str)
        self.wallet_tx.to_json(name, orient = "records", indent = 1)

        print(".json database saved !")

    def update_data(self, name):
        """
        Updating data from  BabiesDegenFlip transactions to an existing json database
        """
        if exists(name) :
            dt_tx = pd.read_json(name, orient = "records") #open the old database
            self.wallet_tx = pd.concat([dt_tx, self.wallet_tx], axis = 0) #concat the rows of new and old data
            self.wallet_tx["timestamp"] = pd.to_datetime(self.wallet_tx["timestamp"]) #convert to date format to avoid bug format
            self.wallet_tx = self.wallet_tx.drop_duplicates("txHash") #remove duplicates if there is (the transaction hash is unique)

            #update of winstreak results
            self.wallet_tx.drop(columns= ["win_streak"], inplace = True)
            self.wallet_tx.sort_values(by = "timestamp", ascending = False, inplace = True)
            self.get_winstreak()

            self.export_data(name)
        else :
            print(".json file not found, creating a new one")
            self.export_data(name)
