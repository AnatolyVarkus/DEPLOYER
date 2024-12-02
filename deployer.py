from pytoniq import StateInit, WalletV4R2, LiteClient, Builder, begin_cell, Cell
from pytoniq_core.boc.address import Address
from tonsdk.utils import to_nano
import asyncio
import time
import config

from random import randint
import logging
from logging.handlers import TimedRotatingFileHandler
import os
import httpx

LOG_DIR = 'logs'
LOG_FILENAME = 'deployer.log'

if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)
    os.chmod(LOG_DIR, 0o777)

log_file_path = os.path.join(LOG_DIR, LOG_FILENAME)

handler = TimedRotatingFileHandler(log_file_path, when='midnight', interval=1, backupCount=1)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(handler)


mnemonics = config.DEPLOYER_MNEMONICS
TON_DURAK_IS_FOREVER = True
next_tx = time.time()
wallet_address = config.DEPLOYER_ADDRESS

provider = LiteClient(
    host="198.244.203.68",
    port=30126,
    server_pub_key="oTIe2ZzV86exWLfm+d4fwVg3SLXL1g47Gqsn7y26Q+U=",
    trust_level=2
)


"""Calculates the contract address based on the user_ids"""
def calculate_contract_address(state_init) -> Address:
    return begin_cell().store_uint(4, 3).store_int(0, 8).store_uint(int.from_bytes(state_init.hash, "big"),
                                                                    256).end_cell().begin_parse().load_address()

"""Combines deployed messages into one transaction for efficiency"""
async def combine_deploy_messages(user_ids, wallet):
    messages = []
    for user_id in user_ids:
        collection_code = Builder.from_boc(config.COLLECTION)[0]
        data = begin_cell().store_int(user_id, 64).end_cell()
        init = StateInit(code=collection_code, data=data)
        address = calculate_contract_address(init.serialize())
        await update_user_address(user_id, address.to_str())
        messages.append(wallet.create_wallet_internal_message(destination=Address(address),
                                                              value=to_nano(0.05, "ton"),
                                                              state_init=init))
    return messages
    pass


"""Deploys wallets and waits for seqno"""
async def deploy_wallets(user_ids):
    await provider.connect()
    wallet = await WalletV4R2.from_mnemonic(provider=provider, mnemonics=mnemonics)

    if await wallet.get_balance() < config.MINIMUM_TON_BALANCE:
        await top_up_deploy_wallet(config.MINIMUM_TON_BALANCE)

    messages = await combine_deploy_messages(user_ids, wallet)
    url = f"https://tonapi.io/v2/blockchain/accounts/{wallet_address}/methods/seqno"
    async with httpx.AsyncClient() as client:
        response = await client.get(url)
        current_seqno = (response.json())["decoded"]["state"]
    await wallet.raw_transfer(messages)
    sleep_amount = 0
    async with httpx.AsyncClient() as client:
        while current_seqno == ((await client.get(url)).json())["decoded"]["state"]:
            await asyncio.sleep(5)
            sleep_amount += 5
            if sleep_amount >= 120:
                raise Exception("Timed out seqno")
    await provider.close()
    pass


"""The main loop, since it's a standalone script that runs 24/7"""
async def main_loop():
    logger.info("Starting")
    while TON_DURAK_IS_FOREVER:
        await asyncio.sleep(1)
        try:
            tx_amount = await get_deploy_tx_amount()
            if tx_amount > 4:
                user_ids = await get_next_deploy_tx(4)
                current_amount_taken = 4
            else:
                user_ids = await get_next_deploy_tx(tx_amount)
                current_amount_taken = tx_amount
            if len(user_ids) > 0:
                try:
                    await deploy_wallets(user_ids)
                    await deploy_queue_update(current_amount_taken)
                    logger.info(f"Deployed accounts: {user_ids}")
                except Exception as e:
                    try:
                        await provider.close()
                    finally:
                        logger.error(e)
            else:
                await asyncio.sleep(10)
        except Exception as e:
            logger.error(e)
    pass


if __name__ == "__main__":
    asyncio.run(main_loop())
