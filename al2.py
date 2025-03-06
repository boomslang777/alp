import os
import math
import discord
import asyncio
import logging
import config
import pytz
import sys
import aiohttp
import signal
import nest_asyncio
from datetime import datetime, time
from alpaca.trading.client import TradingClient
from alpaca.trading.stream import TradingStream
from alpaca.data.live.stock import StockDataStream
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockLatestQuoteRequest
from alpaca.trading.requests import MarketOrderRequest, LimitOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce, OrderStatus
from alpaca.data.enums import DataFeed

# Apply nest_asyncio to allow nested event loops
nest_asyncio.apply()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler('trading.log', mode='w', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# Disable loggers that might interfere with our application
logging.getLogger('websockets').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)

class TelegramLogger:
    """Sends log messages to Telegram for real-time monitoring."""
    def __init__(self, bot_token, chat_id):
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.base_url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        self.session = None

    async def initialize(self):
        """Initialize the aiohttp session."""
        if not self.session:
            self.session = aiohttp.ClientSession()

    async def close(self):
        """Close the aiohttp session."""
        if self.session:
            await self.session.close()
            self.session = None

    async def send_message(self, message):
        """Send a message to the Telegram chat."""
        if not self.session:
            await self.initialize()
        try:
            params = {'chat_id': self.chat_id, 'text': message, 'parse_mode': 'HTML'}
            async with self.session.post(self.base_url, json=params) as response:
                if response.status != 200:
                    logging.error(f"Telegram send failed: {await response.text()}")
        except Exception as e:
            logging.error(f"Error sending Telegram message: {e}")
            
    def _get_est_time(self):
        """Get current time in EST timezone."""
        est = pytz.timezone('US/Eastern')
        return datetime.now(est).strftime('%Y-%m-%d %H:%M:%S EST')
            
    async def send_order_filled(self, symbol, side, quantity, price, order_id):
        """Send a nicely formatted order fill notification."""
        side_text = "BUY" if side == OrderSide.BUY else "SELL"
        
        message = f"""<b>ORDER FILLED: {side_text}</b>

<b>Symbol:</b> {symbol}
<b>Quantity:</b> {quantity}
<b>Price:</b> ${price:.2f}
<b>Total:</b> ${quantity * price:.2f}
<b>Time:</b> {self._get_est_time()}
<b>Order ID:</b> {order_id}"""

        await self.send_message(message)
    
    async def send_stop_loss_triggered(self, symbol, entry_price, high_price, current_price, quantity):
        """Send a stop loss notification with details."""
        drawdown = ((high_price - current_price) / high_price) * 100
        pnl = (current_price - entry_price) * quantity
        pnl_percent = ((current_price - entry_price) / entry_price) * 100
        
        message = f"""<b>STOP LOSS TRIGGERED</b>

<b>Symbol:</b> {symbol}
<b>Current Price:</b> ${current_price:.2f}
<b>Entry Price:</b> ${entry_price:.2f}
<b>Highest Price:</b> ${high_price:.2f}
<b>Drawdown:</b> {drawdown:.2f}%
<b>P&L:</b> ${pnl:.2f} ({pnl_percent:.2f}%)
<b>Time:</b> {self._get_est_time()}"""

        await self.send_message(message)
        
    async def send_exit_filled(self, symbol, entry_price, exit_price, quantity, order_id):
        """Send exit order fill notification with P&L details."""
        pnl = (exit_price - entry_price) * quantity
        pnl_percent = ((exit_price - entry_price) / entry_price) * 100
        
        # Determine result text based on profit or loss
        result = "PROFIT" if pnl > 0 else "LOSS"
        
        message = f"""<b>POSITION CLOSED: {result}</b>

<b>Symbol:</b> {symbol}
<b>Exit Price:</b> ${exit_price:.2f}
<b>Entry Price:</b> ${entry_price:.2f}
<b>Quantity:</b> {quantity}
<b>P&L:</b> ${pnl:.2f} ({pnl_percent:.2f}%)
<b>Time:</b> {self._get_est_time()}
<b>Order ID:</b> {order_id}"""

        await self.send_message(message)
        
    async def send_signal_detected(self, symbol):
        """Send signal detection notification."""
        message = f"""<b>SIGNAL DETECTED</b>

<b>Symbol:</b> {symbol}
<b>Time:</b> {self._get_est_time()}"""

        await self.send_message(message)
    
    async def send_bot_status(self, message, is_error=False):
        """Send bot status update."""
        status_type = "ERROR" if is_error else "STATUS"
        
        formatted_message = f"""<b>BOT {status_type}</b>

{message}
<b>Time:</b> {self._get_est_time()}"""

        await self.send_message(formatted_message)

telegram_logger = TelegramLogger(config.TELEGRAM_BOT_TOKEN, config.TELEGRAM_CHAT_ID)

class OrderManager:
    """Manages order placement with retries for limit orders."""
    def __init__(self, trading_client, historical_client):
        self.trading_client = trading_client
        self.historical_client = historical_client

    async def place_market_order(self, symbol, quantity, side):
        """Place a market order and return the order object."""
        order_request = MarketOrderRequest(
            symbol=symbol,
            qty=quantity,
            side=side,
            time_in_force=TimeInForce.DAY
        )
        try:
            order = self.trading_client.submit_order(order_request)
            logging.info(f"Market {side.name.lower()} order placed for {symbol}: {order.id}")
            return order
        except Exception as e:
            logging.error(f"Failed to place market order for {symbol}: {e}")
            raise

    async def place_limit_order_with_retry(self, symbol, quantity, side, max_attempts=5):
        """Place a limit order with retries if not filled."""
        for attempt in range(max_attempts):
            try:
                price = await self._get_current_price(symbol, side)
                order_request = LimitOrderRequest(
                    symbol=symbol,
                    qty=quantity,
                    side=side,
                    limit_price=price,
                    time_in_force=TimeInForce.DAY,
                    extended_hours=True
                )
                order = self.trading_client.submit_order(order_request)
                logging.info(f"Attempt {attempt + 1}: Limit {side.name.lower()} order placed for {symbol} at ${price:.2f}")
                
                await asyncio.sleep(5)
                order_status = self.trading_client.get_order_by_id(order.id)
                if order_status.status == OrderStatus.FILLED:
                    logging.info(f"Limit order filled for {symbol}")
                    return order
                else:
                    self.trading_client.cancel_order_by_id(order.id)
                    logging.info(f"Order not filled, canceled: {order.id}")
            except Exception as e:
                logging.error(f"Attempt {attempt + 1} failed for {symbol}: {e}")
                if attempt == max_attempts - 1:
                    logging.error(f"Failed to place limit order for {symbol} after {max_attempts} attempts")
                    await telegram_logger.send_message(f"Failed to place limit order for {symbol} after {max_attempts} attempts")
                    raise
        return None

    async def _get_current_price(self, symbol, side):
        """Get the current ask (for buy) or bid (for sell) price."""
        try:
            request = StockLatestQuoteRequest(symbol_or_symbols=[symbol])
            quote = self.historical_client.get_stock_latest_quote(request)
            price = quote[symbol].ask_price if side == OrderSide.BUY else quote[symbol].bid_price
            return price
        except Exception as e:
            logging.error(f"Error getting price for {symbol}: {e}")
            raise

    async def verify_order_execution(self, order_id, max_attempts=5):
        """Verify order execution by checking its status multiple times."""
        for attempt in range(max_attempts):
            try:
                order = self.trading_client.get_order_by_id(order_id)
                logging.info(f"Order {order_id} status check {attempt+1}: {order.status}")
                
                if order.status == OrderStatus.FILLED:
                    logging.info(f"Order {order_id} confirmed FILLED at ${order.filled_avg_price}")
                    return True
                elif order.status in [OrderStatus.CANCELED, OrderStatus.REJECTED, OrderStatus.EXPIRED]:
                    logging.error(f"Order {order_id} failed with status: {order.status}")
                    return False
                
                await asyncio.sleep(2)
            except Exception as e:
                logging.error(f"Error checking order {order_id}: {e}")
                
        logging.error(f"Failed to confirm execution for order {order_id} after {max_attempts} attempts")
        return False

class PositionTracker:
    """Tracks positions and manages real-time price updates for multiple tickers."""
    def __init__(self, trading_bot):
        self.trading_bot = trading_bot
        self.active_positions = {}
        self.data_stream = None
        self.subscribed_symbols = set()
        self.stream_started = False
        self.stream_task = None
        self.subscription_lock = asyncio.Lock()
        self.subscription_queue = asyncio.Queue()
        self.subscription_worker_task = None

    async def start_stream(self):
        """Start the data stream connection."""
        if self.stream_started:
            return
            
        logging.info("Starting data stream with SIP feed")
        self.data_stream = StockDataStream(
            config.ALPACA_API_KEY, 
            config.ALPACA_SECRET_KEY,
            feed=DataFeed.SIP
        )
        
        self.stream_task = asyncio.create_task(self._run_data_stream())
        self.subscription_worker_task = asyncio.create_task(self._subscription_worker())
        self.stream_started = True
        
        await asyncio.sleep(1)
        
    async def _run_data_stream(self):
        """Run the data stream with reconnection logic."""
        while True:
            try:
                logging.info("Connecting to data stream")
                await self.data_stream._run_forever()  # Use internal coroutine
            except Exception as e:
                logging.error(f"Data stream connection error: {e}")
                await telegram_logger.send_bot_status(f"Data stream disconnected: {e}", is_error=True)
            logging.info("Data stream disconnected, reconnecting in 5 seconds...")
            await asyncio.sleep(5)
            
    async def _subscription_worker(self):
        """Background worker that processes subscription requests without blocking."""
        logging.info("Starting subscription worker")
        while True:
            try:
                symbol = await self.subscription_queue.get()
                max_attempts = 3
                
                for attempt in range(max_attempts):
                    try:
                        # Use run_in_executor to make the blocking call non-blocking
                        await asyncio.get_event_loop().run_in_executor(
                            None, 
                            lambda: self.data_stream.subscribe_quotes(self.handle_quote_update, symbol)
                        )
                        
                        logging.info(f"Successfully subscribed to real-time quotes for {symbol}")
                        
                        self.subscription_queue.task_done()
                        break
                    except Exception as e:
                        logging.error(f"Subscription attempt {attempt+1} failed for {symbol}: {e}")
                        if attempt < max_attempts - 1:
                            await asyncio.sleep(2 ** attempt)
                        else:
                            logging.error(f"Failed to subscribe to {symbol} after {max_attempts} attempts")
                            self.subscribed_symbols.discard(symbol)
                            self.subscription_queue.task_done()
            except Exception as e:
                logging.error(f"Error in subscription worker: {e}")
                await asyncio.sleep(1)

    async def subscribe_to_ticker(self, symbol):
        """Subscribe to real-time quotes for a ticker with non-blocking approach."""
        if not self.stream_started:
            await self.start_stream()
        
        if symbol in self.subscribed_symbols:
            return
            
        async with self.subscription_lock:
            if symbol in self.subscribed_symbols:
                return
                
            try:
                # Mark as subscribed first so we don't try again
                self.subscribed_symbols.add(symbol)
                logging.info(f"Scheduled subscription for {symbol}")
                
                # Add to queue for the worker to process
                await self.subscription_queue.put(symbol)
            except Exception as e:
                logging.error(f"Error setting up subscription for {symbol}: {e}")
    
    async def unsubscribe_from_ticker(self, symbol):
        """Unsubscribe from real-time quotes for a ticker if subscribed."""
        if not self.stream_started or symbol not in self.subscribed_symbols:
            return
            
        try:
            if self.data_stream is not None:
                # Use run_in_executor to make the blocking call non-blocking
                await asyncio.get_event_loop().run_in_executor(
                    None, 
                    lambda: self.data_stream.unsubscribe_quotes(symbol)
                )
                
            self.subscribed_symbols.discard(symbol)
            logging.info(f"Unsubscribed from real-time quotes for {symbol}")
        except Exception as e:
            logging.error(f"Error unsubscribing from {symbol}: {e}")

    async def add_position(self, symbol, entry_price, quantity):
        """Add a position, subscribe to its quotes, and log details."""
        if symbol in self.active_positions:
            logging.warning(f"Position already exists for {symbol}")
            return
            
        self.active_positions[symbol] = {
            'entry_price': entry_price,
            'quantity': quantity,
            'highest_price': entry_price,
            'stop_triggered': False,
            'lock': asyncio.Lock(),
            'entry_time': datetime.now()
        }
        
        await self.subscribe_to_ticker(symbol)
        
        log_msg = f"Tracking position for {symbol} started at ${entry_price:.2f}, Qty: {quantity}"
        logging.info(log_msg)
        
        # We don't need to send a separate message here as order_filled will already have sent one
        
        try:
            position = self.trading_bot.trading_client.get_open_position(symbol)
            logging.info(f"API position verification: {symbol} - Avg Price: ${float(position.avg_entry_price):.2f}, Qty: {float(position.qty)}")
        except Exception as e:
            logging.warning(f"Could not verify API position for {symbol}: {e}")

    async def handle_quote_update(self, quote):
        """Process real-time quote updates and check for stop loss."""
        try:
            symbol = quote.symbol
            if symbol not in self.active_positions:
                return
                
            async with self.active_positions[symbol]['lock']:
                position = self.active_positions[symbol]
                
                if position.get('stop_triggered', False):
                    return
                
                mark_price = (quote.bid_price + quote.ask_price) / 2
                
                price_change_pct = abs(mark_price - position.get('last_logged_price', position['entry_price'])) / position['entry_price'] * 100
                
                if 'last_logged_price' not in position or price_change_pct >= 1.0:
                    logging.info(f"Price update for {symbol}: ${mark_price:.2f} (Entry: ${position['entry_price']:.2f})")
                    position['last_logged_price'] = mark_price
                
                if mark_price > position['highest_price']:
                    old_high = position['highest_price']
                    position['highest_price'] = mark_price
                    logging.info(f"New high for {symbol}: ${mark_price:.2f} (previous: ${old_high:.2f})")
                    
                if position['highest_price'] > position['entry_price']:
                    drawdown = ((position['highest_price'] - mark_price) / position['highest_price']) * 100
                    stop_price = position['highest_price'] * (1 - config.STOP_LOSS_PERCENTAGE/100)
                    
                    if mark_price <= stop_price:
                        logging.info(f"Stop loss triggered for {symbol} at ${mark_price:.2f}")
                        logging.info(f"Entry: ${position['entry_price']:.2f}, Highest: ${position['highest_price']:.2f}, Drawdown: {drawdown:.2f}%")
                        
                        position['stop_triggered'] = True
                        await telegram_logger.send_stop_loss_triggered(
                            symbol,
                            position['entry_price'],
                            position['highest_price'],
                            mark_price,
                            position['quantity']
                        )
                        
                        await self.trading_bot.exit_position(symbol, mark_price)
        except Exception as e:
            logging.error(f"Error in quote update handler for {quote.symbol if hasattr(quote, 'symbol') else 'unknown'}: {e}")

    async def remove_position(self, symbol):
        """Remove position from tracking and unsubscribe from quotes."""
        if symbol not in self.active_positions:
            return
            
        try:
            if symbol in self.active_positions:
                position = self.active_positions.pop(symbol)
                logging.info(f"Removed position tracking for {symbol}")
                
            await self.unsubscribe_from_ticker(symbol)
            
            try:
                self.trading_bot.trading_client.get_open_position(symbol)
                logging.warning(f"Warning: Position still exists in API for {symbol}")
            except Exception:
                logging.info(f"Confirmed position closed in API for {symbol}")
                
        except Exception as e:
            logging.error(f"Error removing position for {symbol}: {e}")

    async def check_all_positions(self):
        """Periodic check of all positions to ensure they're properly tracked."""
        try:
            api_positions = self.trading_bot.trading_client.get_all_positions()
            api_symbols = {p.symbol for p in api_positions}
            tracked_symbols = set(self.active_positions.keys())
            
            missing_positions = api_symbols - tracked_symbols
            for symbol in missing_positions:
                position = next((p for p in api_positions if p.symbol == symbol), None)
                if position:
                    logging.warning(f"Found untracked position in API: {symbol}")
                    await self.add_position(
                        symbol, 
                        float(position.avg_entry_price), 
                        int(float(position.qty))
                    )
            
            ghost_positions = tracked_symbols - api_symbols
            for symbol in ghost_positions:
                logging.warning(f"Tracked position not found in API: {symbol}")
                await self.remove_position(symbol)
                
            logging.info(f"Position check completed: {len(self.active_positions)} positions being tracked")
            
        except Exception as e:
            logging.error(f"Error checking positions: {e}")

    async def stop(self):
        """Stop the position tracker and clean up tasks."""
        if self.subscription_worker_task:
            self.subscription_worker_task.cancel()
            try:
                await self.subscription_worker_task
            except asyncio.CancelledError:
                logging.info("Subscription worker task cancelled")
        
        if self.stream_task:
            self.stream_task.cancel()
            try:
                await self.stream_task
            except asyncio.CancelledError:
                logging.info("Data stream task cancelled")
                
        self.stream_started = False

class TradingBot:
    """Core trading logic for signal processing and position management."""
    def __init__(self):
        self.trading_client = TradingClient(config.ALPACA_API_KEY, config.ALPACA_SECRET_KEY, paper=True)
        self.historical_client = StockHistoricalDataClient(config.ALPACA_API_KEY, config.ALPACA_SECRET_KEY)
        self.order_manager = OrderManager(self.trading_client, self.historical_client)
        self.position_tracker = PositionTracker(self)
        self.trade_stream = TradingStream(config.ALPACA_API_KEY, config.ALPACA_SECRET_KEY, paper=True)
        self.lock = asyncio.Lock()
        self.trade_stream_task = None

    async def start(self):
        """Initialize trade and quote streams with proper error handling."""
        try:
            await self.position_tracker.start_stream()
            
            logging.info("Initializing trading stream subscription")
            self.trade_stream.subscribe_trade_updates(self.handle_trade_update)
            
            logging.info("Starting trading stream in background")
            self.trade_stream_task = asyncio.create_task(self._run_trade_stream())
            
            await asyncio.sleep(1)
            logging.info("Trading streams initialized")
        except Exception as e:
            logging.error(f"Error starting trading bot: {e}")
            raise

    async def _run_trade_stream(self):
        """Run the trading stream with reconnection logic."""
        while True:
            try:
                logging.info("Connecting to trading stream")
                await self.trade_stream._run_forever()  # Use internal coroutine
            except Exception as e:
                logging.error(f"Trade stream error: {e}")
                await telegram_logger.send_bot_status(f"Trade stream disconnected: {e}", is_error=True)
            logging.info("Reconnecting to trade stream in 5 seconds...")
            await asyncio.sleep(5)

    async def stop(self):
        """Clean up and stop the trading stream."""
        if self.trade_stream_task:
            self.trade_stream_task.cancel()
            try:
                await self.trade_stream_task
            except asyncio.CancelledError:
                logging.info("Trade stream task cancelled")

        # Also stop the position tracker
        await self.position_tracker.stop()

    def is_market_hours(self):
        """Check if current time is within US market hours (9:30 AM - 4:00 PM EST)."""
        est = pytz.timezone('US/Eastern')
        now = datetime.now(est)
        return time(9, 30) <= now.time() <= time(16, 0)

    async def handle_trade_update(self, data):
        """Process trade updates and start tracking filled buy orders."""
        try:
            if not hasattr(data, 'order'):
                logging.warning(f"Trade update missing order data: {data}")
                return
                
            order = data.order
            logging.info(f"Trade update: {order.symbol} - {data.event}")
            
            if data.event == 'fill' and order.side == OrderSide.BUY:
                await self.position_tracker.add_position(
                    order.symbol,
                    float(order.filled_avg_price),
                    int(float(order.filled_qty))
                )
            elif data.event == 'fill' and order.side == OrderSide.SELL:
                await self.position_tracker.remove_position(order.symbol)
        except Exception as e:
            logging.error(f"Error processing trade update: {e}")

    async def process_signal(self, symbol):
        """Handle a trading signal based on market hours."""
        try:
            async with self.lock:
                logging.info(f"Processing signal for {symbol}")
                await telegram_logger.send_signal_detected(symbol)
                
                try:
                    self.trading_client.get_open_position(symbol)
                    logging.info(f"Position already exists for {symbol}, skipping signal")
                    await telegram_logger.send_bot_status(f"Position already exists for {symbol}, skipping signal")
                    return
                except Exception:
                    pass
                
                quantity = await self.calculate_quantity(symbol)
                if self.is_market_hours():
                    order = await self.order_manager.place_market_order(symbol, quantity, OrderSide.BUY)
                else:
                    order = await self.order_manager.place_limit_order_with_retry(symbol, quantity, OrderSide.BUY)
                    
                logging.info(f"Order placed for {symbol}: {order.id}")
                
                # Verify order execution before sending notification
                success = await self.order_manager.verify_order_execution(order.id)
                
                if success:
                    # Get the filled order details for the notification
                    filled_order = self.trading_client.get_order_by_id(order.id)
                    await telegram_logger.send_order_filled(
                        symbol,
                        OrderSide.BUY,
                        int(float(filled_order.filled_qty)),
                        float(filled_order.filled_avg_price),
                        order.id
                    )
                
        except Exception as e:
            logging.error(f"Failed to process signal for {symbol}: {e}")
            await telegram_logger.send_bot_status(f"Failed to process signal for {symbol}: {e}", is_error=True)

    async def exit_position(self, symbol, current_price=None):
        """Exit a position based on market hours."""
        try:
            try:
                position = self.trading_client.get_open_position(symbol)
                quantity = int(float(position.qty))
                entry_price = float(position.avg_entry_price)
            except Exception as e:
                logging.warning(f"Could not get position details from API: {e}")
                if symbol in self.position_tracker.active_positions:
                    position_data = self.position_tracker.active_positions[symbol]
                    quantity = position_data['quantity']
                    entry_price = position_data['entry_price']
                else:
                    logging.error(f"No position data found for {symbol}")
                    return
                    
            if current_price:
                pnl = (current_price - entry_price) * quantity
                pnl_percent = (current_price - entry_price) / entry_price * 100
                logging.info(f"Exiting position for {symbol} - P&L: ${pnl:.2f} ({pnl_percent:.2f}%)")
            
            try:
                if self.is_market_hours():
                    order = await self.order_manager.place_market_order(symbol, quantity, OrderSide.SELL)
                else:
                    order = await self.order_manager.place_limit_order_with_retry(symbol, quantity, OrderSide.SELL)
                
                logging.info(f"Exit order placed for {symbol}: {order.id}")
                
                success = await self.order_manager.verify_order_execution(order.id)
                if success:
                    # Get the filled order details for the notification
                    filled_order = self.trading_client.get_order_by_id(order.id)
                    await telegram_logger.send_exit_filled(
                        symbol,
                        entry_price,
                        float(filled_order.filled_avg_price),
                        quantity,
                        order.id
                    )
                    await self.position_tracker.remove_position(symbol)
                else:
                    logging.error(f"Exit order for {symbol} did not execute properly")
                    await telegram_logger.send_bot_status(f"Exit order for {symbol} did not execute properly", is_error=True)
                
            except Exception as e:
                logging.error(f"Failed to place exit order for {symbol}: {e}")
                await telegram_logger.send_bot_status(f"Failed to exit position for {symbol}: {e}", is_error=True)
        except Exception as e:
            logging.error(f"Error in exit_position for {symbol}: {e}")
            await telegram_logger.send_bot_status(f"Error in exit_position for {symbol}: {e}", is_error=True)

    async def calculate_quantity(self, symbol):
        """Calculate the number of shares to buy based on investment amount."""
        try:
            price = await self.order_manager._get_current_price(symbol, OrderSide.BUY)
            qty = math.floor(config.AMOUNT_TO_INVEST / price)
            
            if qty < 1:
                qty = 1
                logging.warning(f"Adjusted quantity for {symbol} to minimum 1 share (price: ${price:.2f})")
                
            logging.info(f"Calculated quantity for {symbol}: {qty} shares at ${price:.2f}")
            return qty
        except Exception as e:
            logging.error(f"Error calculating quantity for {symbol}: {e}")
            raise

class DiscordClient(discord.Client):
    """Listens for trading signals from Discord."""
    def __init__(self, trading_bot):
        intents = discord.Intents.default()
        intents.message_content = True
        super().__init__(intents=intents)
        self.trading_bot = trading_bot
        self.lock = asyncio.Lock()

    async def on_ready(self):
        logging.info(f"Logged in as {self.user}")
        await self.trading_bot.start()

    async def on_message(self, message):
        """Process messages for trading signals without logging every message."""
        if message.author == self.user or 'day-trading' not in message.channel.name:
            return
            
        content = message.content
        if '↑' in content or '↗' in content:
            async with self.lock:
                try:
                    parts = content.split()
                    symbol = None
                    for i, part in enumerate(parts):
                        if part in ['↑', '↗'] and i + 1 < len(parts):
                            next_part = parts[i + 1]
                            if next_part.startswith('**') and next_part.endswith('**'):
                                symbol = next_part[2:-2].upper()
                            elif next_part.isalpha() and 1 <= len(next_part) <= 5:
                                symbol = next_part.upper()
                            if symbol:
                                await self.trading_bot.process_signal(symbol)
                                break
                except Exception as e:
                    logging.error(f"Failed to parse trading signal: {e}")

async def main():
    """Run the trading bot."""
    trading_bot = TradingBot()
    client = DiscordClient(trading_bot)
    try:
        await telegram_logger.initialize()
        await telegram_logger.send_bot_status("Trading Bot Started")
        logging.info("Starting trading bot")
        
        # Create a task for the client
        client_task = asyncio.create_task(client.start(config.DISCORD_TOKEN))
        
        # Set up signal handlers for graceful shutdown on CTRL+C
        def signal_handler():
            logging.info("Shutdown signal received, closing bot...")
            client_task.cancel()
        
        try:
            # Add signal handlers for graceful termination
            loop = asyncio.get_running_loop()
            for signal_name in ('SIGINT', 'SIGTERM'):
                if sys.platform != 'win32':  # SIGTERM is not supported on Windows
                    loop.add_signal_handler(
                        getattr(signal, signal_name),
                        signal_handler
                    )
        except NotImplementedError:
            # Signal handlers not supported on this platform
            pass
            
        # Wait for the client task to complete
        await client_task
    except asyncio.CancelledError:
        logging.info("Bot shutdown initiated")
    except Exception as e:
        logging.error(f"Fatal error: {e}")
        await telegram_logger.send_bot_status(f"Bot crashed: {e}", is_error=True)
    finally:
        # Ensure proper cleanup
        logging.info("Shutting down bot...")
        await trading_bot.stop()
        await client.close()
        await telegram_logger.close()
        logging.info("Bot shutdown complete")

if __name__ == "__main__":
    asyncio.run(main())