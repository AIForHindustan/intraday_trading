#!/usr/bin/env python3
"""
Market Calendar Utility
======================

Provides trading day checking and expiry date calculation functionality 
for NSE (National Stock Exchange) India using pandas_market_calendars.
"""

from datetime import datetime, timedelta
from typing import Optional, List
import logging

logger = logging.getLogger(__name__)

# Try to import pandas_market_calendars
try:
    import pandas_market_calendars as mcal
    CALENDAR_AVAILABLE = True
except ImportError:
    CALENDAR_AVAILABLE = False
    logger.warning("pandas_market_calendars not available, using basic weekday check")


class MarketCalendar:
    """
    Market calendar for checking trading days and calculating expiry dates.
    
    Uses pandas_market_calendars for accurate NSE holiday calendar.
    Includes methods for weekly and monthly expiry date calculations.
    """
    
    def __init__(self, exchange: str = 'NSE'):
        """
        Initialize market calendar.
        
        Args:
            exchange: Exchange name (default: 'NSE' for National Stock Exchange India)
        """
        self.exchange = exchange
        self.calendar = None
        
        if CALENDAR_AVAILABLE:
            try:
                self.calendar = mcal.get_calendar(exchange)
                logger.info(f"✅ Market calendar initialized for {exchange}")
            except Exception as e:
                logger.warning(f"⚠️ Failed to load {exchange} calendar: {e}")
                self.calendar = None
        else:
            logger.warning("⚠️ Using basic weekday checking (pandas_market_calendars not available)")
    
    def is_trading_day(self, date: Optional[datetime] = None) -> bool:
        """
        Check if a given date is a trading day.
        
        Args:
            date: Datetime object to check. If None, uses current date.
            
        Returns:
            True if the date is a trading day, False otherwise.
        """
        if date is None:
            date = datetime.now()
        
        # Use calendar if available
        if self.calendar:
            try:
                schedule = self.calendar.schedule(
                    start_date=date.date(),
                    end_date=date.date()
                )
                return len(schedule) > 0
            except Exception as e:
                logger.debug(f"Error checking trading day with calendar: {e}")
                # Fallback to basic weekday check
                return date.weekday() < 5
        
        # Fallback: Basic weekday check (Monday=0 to Friday=4)
        return date.weekday() < 5
    
    def get_next_weekly_expiry(self, from_date: Optional[datetime] = None) -> Optional[datetime]:
        """
        Get the next weekly expiry date (Thursday).
        
        Args:
            from_date: Starting date (default: current date)
            
        Returns:
            Next weekly expiry datetime, or None if not found
        """
        if from_date is None:
            from_date = datetime.now()
        
        # Find next Thursday
        days_ahead = 3 - from_date.weekday()  # 3 = Thursday
        if days_ahead <= 0:  # Target day already happened this week
            days_ahead += 7
        
        potential_expiry = from_date + timedelta(days=days_ahead)
        
        # Ensure it's a trading day
        if self.calendar:
            # Find next trading day if expiry falls on holiday
            max_attempts = 10
            attempts = 0
            while attempts < max_attempts and not self.is_trading_day(potential_expiry):
                potential_expiry += timedelta(days=1)
                attempts += 1
        else:
            # Basic check: if Thursday is weekend, move to next Thursday
            if potential_expiry.weekday() >= 5:
                potential_expiry += timedelta(days=7)
        
        return potential_expiry
    
    def get_next_monthly_expiry(self, from_date: Optional[datetime] = None) -> Optional[datetime]:
        """
        Get the next monthly expiry date (last Thursday of month).
        
        Args:
            from_date: Starting date (default: current date)
            
        Returns:
            Next monthly expiry datetime, or None if not found
        """
        if from_date is None:
            from_date = datetime.now()
        
        # Start from first day of next month
        if from_date.month == 12:
            next_month = from_date.replace(year=from_date.year + 1, month=1, day=1)
        else:
            next_month = from_date.replace(month=from_date.month + 1, day=1)
        
        # Get last day of that month
        if next_month.month == 12:
            last_day = next_month.replace(day=31)
        else:
            last_day = (next_month.replace(month=next_month.month + 1, day=1) - timedelta(days=1))
        
        # Find last Thursday
        current = last_day
        while current.weekday() != 3:  # 3 = Thursday
            current -= timedelta(days=1)
        
        # Ensure it's a trading day
        if not self.is_trading_day(current):
            # Move to previous trading day
            while not self.is_trading_day(current):
                current -= timedelta(days=1)
        
        return current
    
    def get_all_upcoming_expiries(self, days_lookahead: int = 30, 
                                  from_date: Optional[datetime] = None) -> List[datetime]:
        """
        Get all upcoming expiry dates (weekly and monthly) within lookahead period.
        
        Args:
            days_lookahead: Number of days to look ahead
            from_date: Starting date (default: current date)
            
        Returns:
            List of expiry datetime objects
        """
        if from_date is None:
            from_date = datetime.now()
        
        end_date = from_date + timedelta(days=days_lookahead)
        expiries = []
        
        if self.calendar:
            try:
                schedule = self.calendar.schedule(
                    start_date=from_date.date(),
                    end_date=end_date.date()
                )
                
                # Filter Thursdays (typical expiry days)
                for date in schedule.index:
                    if date.weekday() == 3:  # Thursday
                        expiries.append(datetime.combine(date, datetime.min.time()))
            except Exception as e:
                logger.debug(f"Error getting expiry dates: {e}")
        else:
            # Fallback: Find all Thursdays in range
            current = from_date
            while current <= end_date:
                if current.weekday() == 3:  # Thursday
                    expiries.append(current)
                current += timedelta(days=1)
        
        return expiries
    
    def is_expiry_day(self, date: Optional[datetime] = None) -> bool:
        """
        Check if a given date is an expiry day (Thursday).
        
        Args:
            date: Datetime object to check. If None, uses current date.
            
        Returns:
            True if the date is an expiry day, False otherwise.
        """
        if date is None:
            date = datetime.now()
        
        # Expiry days are typically Thursdays
        return date.weekday() == 3 and self.is_trading_day(date)
    
    def get_trading_days_between(self, start_date: datetime, end_date: datetime) -> int:
        """
        Get number of trading days between two dates.
        
        Args:
            start_date: Start date
            end_date: End date
            
        Returns:
            Number of trading days (inclusive)
        """
        if self.calendar:
            try:
                schedule = self.calendar.schedule(
                    start_date=start_date.date(),
                    end_date=end_date.date()
                )
                return len(schedule)
            except Exception as e:
                logger.debug(f"Error calculating trading days: {e}")
        
        # Fallback: Count weekdays
        count = 0
        current = start_date
        while current <= end_date:
            if current.weekday() < 5:
                count += 1
            current += timedelta(days=1)
        return count

