# client.py

class Client:
    """
    Represents a client with associated attributes and methods.
    """

    def __init__(self, chat_id, firstname, lastname, lang, status: dict) -> None:
        """
        Initializes a Client object with provided attributes.

        Args:
        - chat_id (int or str): Unique identifier for the client.
        - firstname (str): First name of the client.
        - lastname (str): Last name of the client.
        - lang (str): Language preference of the client.
        - status (dict): Dictionary containing client status information.
        """
        self.chat_id = chat_id
        self.firstname = firstname
        self.lastname = lastname
        self.lang = lang
        self.balance = 0  # Initialize balance to 0
        self.status = status
        self.active_deposit_address = ""  # Initialize active deposit address as empty string

    def set_status(self, status: dict) -> None:
        """
        Sets the status dictionary for the client.

        Args:
        - status (dict): Dictionary containing client status information.
        """
        self.status = status

    def get_status(self) -> dict:
        """
        Retrieves the current status dictionary of the client.

        Returns:
        - dict: Dictionary containing client status information.
        """
        return self.status

    def set_active_deposit_address(self, deposit_address: str) -> None:
        """
        Sets the active deposit address for the client.

        Args:
        - deposit_address (str): Active deposit address to set.
        """
        self.active_deposit_address = deposit_address

    def get_active_deposit_address(self) -> str:
        """
        Retrieves the active deposit address of the client.

        Returns:
        - str: Active deposit address of the client.
        """
        return self.active_deposit_address

    def __str__(self) -> str:
        """
        Returns a string representation of the Client object.

        Returns:
        - str: String representation containing chat_id, firstname, lastname, and lang.
        """
        return f"Client(chat_id={self.chat_id}, firstname='{self.firstname}', lastname='{self.lastname}', lang='{self.lang}')"
