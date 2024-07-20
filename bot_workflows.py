# bot_workflow.py

class Workflows:
    class Start:
        MENU_0 = {
                    'description': 'go to start menu',
                    'function': 'start',
                }
    class Idle:
        IDLE_0 = {
                    'description': 'default state',
                    'function': '',
                }
    class RequestDeposit:
        SDA_0 = {
                    'description': 'show deposit address',
                    'function': 'show_deposit_address',
                }
        WFD_1 = {
                    'description': 'waiting for deposit',
                    'function': 'poll_deposit',
                }
    class GetBalance:
        GEB_0 = {
                    'description': 'show balance',
                    'function': 'show_balance',
                }
    class Withdraw:
        WDR_0 = {
                    'description': 'withdraw funds',
                    'function': 'request_withdrawal',
                }
    class GetHelp:
        HLP_0 = {
            'description': 'show list of available bot commands',
            'function': 'show_command_list',
        }
