
from request_tx import BabiesDegenFlipTx

date_from = "2022-02-26 19:00:00"
date_to = "2022-03-11 20:00:00"

BDF = BabiesDegenFlipTx(date_to = date_to, date_from = date_from)

BDF.get_all_tx()
print("Win or Lose status :")
BDF.get_wallet_WoL()
BDF.get_winstreak()
BDF.update_data("BabiesDegenFlipTX.json")




