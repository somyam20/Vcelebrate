import asyncio
import logging
from datetime import datetime, timedelta
from src.utils.inventory_processor import check_and_update_inventory

logger = logging.getLogger(__name__)


class InventoryScheduler:
    """
    Simple scheduler to automatically run inventory updates on the 1st of each month.
    No external dependencies - uses asyncio.
    """
    
    def __init__(self):
        self._task = None
        self._running = False
        self._last_run_month = None
    
    async def run_monthly_inventory_update(self):
        """
        Run the monthly inventory update job.
        This is called automatically on the 1st of each month.
        """
        try:
            current_date = datetime.now()
            logger.info("=" * 80)
            logger.info(f"SCHEDULED MONTHLY INVENTORY UPDATE - {current_date.strftime('%B %d, %Y')}")
            logger.info("=" * 80)
            
            # Run the inventory update for current month
            result = await check_and_update_inventory()
            
            if result.get("status") == "success":
                logger.info("Monthly inventory update completed successfully")
                logger.info(f"Summary: {result.get('total_birthdays', 0)} birthdays, "
                          f"{result.get('total_anniversaries', 0)} anniversaries, "
                          f"{result.get('total_service_completions', 0)} service completions")
                
                # Log low inventory alerts
                alerts = result.get('low_inventory_alerts', [])
                if alerts:
                    logger.warning(f"LOW INVENTORY ALERTS: {len(alerts)} items below threshold")
                    for alert in alerts:
                        logger.warning(f"  - {alert['workbook']} at {alert['location']}: "
                                     f"{alert['current_quantity']} units")
            else:
                logger.error(f"Monthly inventory update failed: {result.get('message', 'Unknown error')}")
            
            logger.info("=" * 80)
            
        except Exception as e:
            logger.exception(f"Error in scheduled inventory update: {e}")
    
    async def _scheduler_loop(self):
        """Main scheduler loop that checks every hour if it's time to run"""
        logger.info("Inventory scheduler loop started")
        
        while self._running:
            try:
                current_date = datetime.now()
                current_day = current_date.day
                current_month = current_date.month
                current_hour = current_date.hour
                
                # Run on the 1st of each month at 00:xx (between midnight and 1 AM)
                # And only if we haven't run this month yet
                if (current_day == 1 and 
                    current_hour == 0 and 
                    self._last_run_month != current_month):
                    
                    logger.info(f"Triggering monthly inventory update for {current_date.strftime('%B %Y')}")
                    await self.run_monthly_inventory_update()
                    self._last_run_month = current_month
                
                # Sleep for 1 hour before checking again
                await asyncio.sleep(3600)
                
            except asyncio.CancelledError:
                logger.info("Scheduler loop cancelled")
                break
            except Exception as e:
                logger.exception(f"Error in scheduler loop: {e}")
                # Sleep for 1 hour even on error
                await asyncio.sleep(3600)
    
    def start(self):
        """Start the scheduler"""
        if self._running:
            logger.warning("Scheduler is already running")
            return
        
        self._running = True
        self._task = asyncio.create_task(self._scheduler_loop())
        
        next_check = datetime.now() + timedelta(hours=1)
        logger.info(f"Inventory scheduler started. Will check for updates on 1st of each month.")
        logger.info(f"Next check: {next_check.strftime('%Y-%m-%d %H:%M:%S')}")
    
    def stop(self):
        """Stop the scheduler"""
        if not self._running:
            return
        
        self._running = False
        if self._task:
            self._task.cancel()
        logger.info("Inventory scheduler stopped")
    
    async def run_now(self):
        """Manually trigger the inventory update (for testing or manual runs)"""
        logger.info("Manually triggering inventory update...")
        await self.run_monthly_inventory_update()


# Global scheduler instance
inventory_scheduler = InventoryScheduler()


# Convenience functions
def start_scheduler():
    """Start the global scheduler"""
    inventory_scheduler.start()


def stop_scheduler():
    """Stop the global scheduler"""
    inventory_scheduler.stop()


async def manual_inventory_update():
    """Manually trigger an inventory update"""
    await inventory_scheduler.run_now()