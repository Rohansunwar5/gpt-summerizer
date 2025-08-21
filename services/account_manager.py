import json
import os
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional

logger = logging.getLogger(__name__)

class AccountRotationManager:
    def __init__(self, accounts_file: str = 'telegram_accounts.json'):
        self.accounts_file = accounts_file
        self.current_account_index = 0
        self.rate_limit_cooldown = {}
    
    def _load_accounts_from_file(self) -> Tuple[List[Dict], int]:
        """Load accounts from JSON file"""
        try:
            if os.path.exists(self.accounts_file):
                with open(self.accounts_file, 'r') as f:
                    data = json.load(f)
                    return data.get('accounts', []), data.get('current_index', 0)
            return [], 0
        except Exception as e:
            logger.error(f"Error loading accounts from file: {str(e)}")
            return [], 0
    
    def _save_accounts_to_file(self, accounts: List[Dict], current_index: int):
        """Save accounts to JSON file"""
        try:
            data = {
                'accounts': accounts,
                'current_index': current_index,
                'updated_at': datetime.now().isoformat()
            }
            with open(self.accounts_file, 'w') as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            logger.error(f"Error saving accounts to file: {str(e)}")
    
    def load_accounts(self) -> Tuple[List[Dict], int]:
        """Public method to load accounts"""
        return self._load_accounts_from_file()
    
    def save_accounts(self, accounts: List[Dict], current_index: int):
        """Public method to save accounts"""
        self._save_accounts_to_file(accounts, current_index)
    
    def get_next_available_account(self) -> Tuple[Dict, int]:
        """Get the next available account that's not rate limited"""
        accounts, current_index = self.load_accounts()
        
        if not accounts:
            raise Exception("No Telegram accounts configured")
        
        # Find available accounts
        available_accounts = []
        now = datetime.now()
        
        for i, account in enumerate(accounts):
            rate_limited_until = account.get('rate_limited_until', '')
            
            if rate_limited_until:
                try:
                    rate_limit_end = datetime.fromisoformat(rate_limited_until)
                    if now > rate_limit_end:
                        # Rate limit expired
                        account['rate_limited_until'] = ''
                        available_accounts.append((i, account))
                except:
                    available_accounts.append((i, account))
            else:
                available_accounts.append((i, account))
        
        if not available_accounts:
            raise Exception("All accounts are currently rate limited. Please wait.")
        
        # Select account with least usage
        available_accounts.sort(key=lambda x: x[1].get('usage_count', 0))
        selected_index, selected_account = available_accounts[0]
        
        return selected_account, selected_index
    
    def update_account_usage(self, account_index: int):
        """Update account usage statistics"""
        accounts, current_index = self.load_accounts()
        
        if 0 <= account_index < len(accounts):
            accounts[account_index]['last_used'] = datetime.now().isoformat()
            accounts[account_index]['usage_count'] = accounts[account_index].get('usage_count', 0) + 1
            self.save_accounts(accounts, current_index)
    
    def mark_account_rate_limited(self, account_index: int, wait_seconds: int = 3600):
        """Mark an account as rate limited"""
        accounts, current_index = self.load_accounts()
        
        if 0 <= account_index < len(accounts):
            rate_limited_until = datetime.now() + timedelta(seconds=wait_seconds)
            accounts[account_index]['rate_limited_until'] = rate_limited_until.isoformat()
            
            logger.warning(f"Account {account_index} rate limited until {rate_limited_until}")
            self.save_accounts(accounts, current_index)
    
    def get_accounts_status(self) -> Dict:
        """Get status of all accounts"""
        accounts, current_index = self.load_accounts()
        account_status = []
        now = datetime.now()
        
        for i, account in enumerate(accounts):
            rate_limited_until = account.get('rate_limited_until', '')
            is_rate_limited = False
            
            if rate_limited_until:
                try:
                    rate_limit_end = datetime.fromisoformat(rate_limited_until)
                    is_rate_limited = now <= rate_limit_end
                except:
                    is_rate_limited = False
            
            account_status.append({
                'index': i,
                'phone_number': account.get('phone_number', 'N/A'),
                'last_used': account.get('last_used', 'Never'),
                'usage_count': account.get('usage_count', 0),
                'is_rate_limited': is_rate_limited,
                'rate_limited_until': rate_limited_until if is_rate_limited else None
            })
        
        return {
            'total_accounts': len(accounts),
            'current_index': current_index,
            'accounts': account_status
        }
    
    def reset_rate_limits(self):
        """Reset rate limits for all accounts"""
        accounts, current_index = self.load_accounts()
        
        for account in accounts:
            account['rate_limited_until'] = ''
        
        self.save_accounts(accounts, current_index)
        logger.info("Rate limits reset for all accounts")