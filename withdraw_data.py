# withdraw_data.py

import logging

# Set up logging configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ClientWithdrawal:
    def __init__(self) -> None:
        """
        Initialize an instance of ClientWithdrawal.
        """
        self.withdrawals = []


    def update_amount(self, chat_id, amount):
        """
        Update the withdrawal amount for a specific chat_id.

        Args:
            chat_id (int): The ID of the chat or user.
            amount (float): The amount to update or set for withdrawal.

        """
        # Check if the chat_id exists in self.withdrawals
        for withdrawal in self.withdrawals:
            if withdrawal['chat_id'] == chat_id:
                # If it exists, update the amount
                withdrawal['amount'] = amount
                logger.info(f"Updated withdrawal amount for chat_id {chat_id} to {amount}")
                return
        
        # If the chat_id does not exist, create a new withdrawal
        new_withdrawal = {'chat_id': chat_id, 'amount': amount, 'wallet': ''}
        self.withdrawals.append(new_withdrawal)
        logger.info(f"Created new withdrawal entry for chat_id {chat_id} with amount {amount}")


    def update_wallet(self, chat_id, wallet: str):
        """
        Update the withdrawal wallet address for a specific chat_id.

        Args:
            chat_id (int): The ID of the chat or user.
            wallet (str): The wallet address to update or set for withdrawal.

        """
        # Check if the chat_id exists in self.withdrawals
        for withdrawal in self.withdrawals:
            if withdrawal['chat_id'] == chat_id:
                # If it exists, update the wallet
                withdrawal['wallet'] = wallet
                logger.info(f"Updated withdrawal wallet for chat_id {chat_id} to {wallet}")
                return
        
        # If the chat_id does not exist, create a new withdrawal
        new_withdrawal = {'chat_id': chat_id, 'amount': 0, 'wallet': wallet}
        self.withdrawals.append(new_withdrawal)
        logger.info(f"Created new withdrawal entry for chat_id {chat_id} with wallet {wallet}")


    def get_withdrawal_data(self, chat_id):
        """
        Retrieve the withdrawal data (amount and wallet) for a specific chat_id.

        Args:
            chat_id (int): The ID of the chat or user.

        Returns:
            dict or None: The withdrawal data as a dictionary containing 'amount' and 'wallet',
                          or None if the chat_id does not exist.

        """
        # Check if the chat_id exists in self.withdrawals
        for withdrawal in self.withdrawals:
            if withdrawal['chat_id'] == chat_id:
                # Return the complete withdrawal data (amount and wallet)
                logger.info(f"Retrieved withdrawal data for chat_id {chat_id}: {withdrawal}")
                return withdrawal
        # If the chat_id does not exist, return None
        logger.warning(f"Requested withdrawal data for chat_id {chat_id} does not exist")
        return None


    def remove_withdrawal(self, chat_id):
        """
        Remove a withdrawal entry for a specific chat_id.

        Args:
            chat_id (int): The ID of the chat or user.

        Returns:
            bool: True if the withdrawal was successfully removed, False if not found.

        """
        # Check if the chat_id exists in self.withdrawals and remove it
        for i, withdrawal in enumerate(self.withdrawals):
            if withdrawal['chat_id'] == chat_id:
                self.withdrawals.pop(i)
                logger.info(f"Removed withdrawal entry for chat_id {chat_id}")
                return True  # Indicate that the withdrawal was removed
        logger.warning(f"Failed to remove withdrawal entry for chat_id {chat_id}: Entry not found")
        return False  # Indicate that the withdrawal was not found
