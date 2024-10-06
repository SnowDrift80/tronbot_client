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
        CAR_2 = {
                    'description': 'asking client if he has a referral code',
                    'function': 'client_ask_referral'
                }
        ERC_3 = {
                    'description': 'enter referral code',
                    'function': 'enter_referral_code'
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
    class GotoChat:
        GOC_0 = {
                    'description': 'go to group chat',
                    'function': 'goto_groupchat',
                }
    class GetStatistics:
        GES_0 = {
                    'description': 'get returns statistics',
                    'function': 'get_statistics',
                }
    class GotoFAQ:
        GOF_0 = {
                    'description': 'view frequently asked questions',
                    'function': 'view_faq',
                }
    class ContactSupport:
        COS_0 = {
                    'description': 'contact support',
                    'function': 'contact_support',
                }
    class GetReferralCode:
        GRC_0 = {
                    'description': 'update and get referral code',
                    'function': 'show_referral_code',
                }
