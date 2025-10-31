"""
Alerts Package
==============

Consolidated alert management system with filtering and notifications.
Contains alert processing, filtering logic, and notification templates.

Files:
- alert_manager.py: Consolidated alert management
- filters.py: Alert filtering logic (6-path qualification system)
- notifiers.py: Telegram/Redis notifications with templates

Created: October 9, 2025
"""

# Avoid eager imports to keep package lightweight for dashboard runtime.
# Import submodules directly where needed, e.g. `from alerts.alert_manager import AlertManager`.

__all__ = []
