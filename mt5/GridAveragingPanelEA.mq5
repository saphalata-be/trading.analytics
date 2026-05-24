//+------------------------------------------------------------------+
//|                                           GridAveragingPanelEA.mq5 |
//|                         Expert Advisor for manual grid averaging |
//+------------------------------------------------------------------+
#property copyright "trading.analytics"
#property version   "1.00"
#property strict

#include <Trade/Trade.mqh>

input long   InpMagicNumber        = 26052201;
input double InpLots               = 0.01;
input int    InpGridDistancePoints = 1000;
input int    InpPendingOrders      = 3;
input double InpTakeProfitMoney    = 10.0;
input int    InpMaxLevels          = 8;
input int    InpMaxSlippagePoints  = 20;
input int    InpMaxSpreadPoints    = 50;

enum AtrStrategyMode
{
   ATR_STRATEGY_D1_50 = 0,      // Vrai ATR(50) D1
   ATR_STRATEGY_FIXED_500 = 500, // ATR fixe 500 points
   ATR_STRATEGY_FIXED_1000 = 1000 // ATR fixe 1000 points
};

input AtrStrategyMode InpAtrStrategyMode = ATR_STRATEGY_D1_50;

enum TradeSide
{
   SIDE_BUY = 0,
   SIDE_SELL = 1
};

struct PanelSettings
{
   TradeSide side;
   double lots;
   int grid_points;
   int pending_count;
   double take_profit_money;
   int max_levels;
   int max_slippage_points;
   int max_spread_points;
};

struct LossProjection
{
   double profit;
   double target_price;
};

CTrade trade;

const string PREFIX = "GA_PANEL_";
const int PANEL_X = 10;
const int PANEL_Y = 20;
const int PANEL_W = 330;
const int ROW_H = 22;
const int LABEL_W = 170;
const int VALUE_W = 135;

PanelSettings settings;
int atr_handle = INVALID_HANDLE;
datetime last_panel_update = 0;
string last_status = "";
bool side_dropdown_open = false;

//+------------------------------------------------------------------+
//| Utility helpers                                                  |
//+------------------------------------------------------------------+
string ObjName(const string suffix)
{
   return PREFIX + suffix;
}

double NormalizeVolume(const double requested_volume)
{
   double min_volume = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_MIN);
   double max_volume = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_MAX);
   double step = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_STEP);

   if(step <= 0.0)
      step = min_volume;

   double volume = MathMax(min_volume, MathMin(max_volume, requested_volume));
   volume = MathFloor(volume / step + 0.0000001) * step;

   int digits = 2;
   if(step > 0.0)
      digits = (int)MathMax(0, MathCeil(-MathLog10(step)));

   return NormalizeDouble(volume, digits);
}

double NormalizePrice(const double price)
{
   return NormalizeDouble(price, (int)SymbolInfoInteger(_Symbol, SYMBOL_DIGITS));
}

int CurrentSpreadPoints()
{
   long spread = SymbolInfoInteger(_Symbol, SYMBOL_SPREAD);
   return (int)spread;
}

bool IsSpreadAllowed()
{
   return CurrentSpreadPoints() <= settings.max_spread_points;
}

bool IsOurPosition()
{
   if(PositionGetString(POSITION_SYMBOL) != _Symbol)
      return false;
   return (long)PositionGetInteger(POSITION_MAGIC) == InpMagicNumber;
}

bool IsOurOrder()
{
   if(OrderGetString(ORDER_SYMBOL) != _Symbol)
      return false;
   return (long)OrderGetInteger(ORDER_MAGIC) == InpMagicNumber;
}

int PositionsCount()
{
   int count = 0;
   for(int i = PositionsTotal() - 1; i >= 0; --i)
   {
      ulong ticket = PositionGetTicket(i);
      if(ticket == 0 || !PositionSelectByTicket(ticket))
         continue;
      if(IsOurPosition())
         count++;
   }
   return count;
}

int PendingOrdersCount()
{
   int count = 0;
   for(int i = OrdersTotal() - 1; i >= 0; --i)
   {
      ulong ticket = OrderGetTicket(i);
      if(ticket == 0 || !OrderSelect(ticket))
         continue;
      if(IsOurOrder())
         count++;
   }
   return count;
}

int TotalManagedLevels()
{
   return PositionsCount() + PendingOrdersCount();
}

bool HasManagedCycle()
{
   return TotalManagedLevels() > 0;
}

double TotalNetProfit()
{
   double profit = 0.0;
   for(int i = PositionsTotal() - 1; i >= 0; --i)
   {
      ulong ticket = PositionGetTicket(i);
      if(ticket == 0 || !PositionSelectByTicket(ticket))
         continue;
      if(!IsOurPosition())
         continue;

      profit += PositionGetDouble(POSITION_PROFIT);
      profit += PositionGetDouble(POSITION_SWAP);
   }
   return profit;
}

TradeSide ActiveCycleSide()
{
   for(int i = PositionsTotal() - 1; i >= 0; --i)
   {
      ulong ticket = PositionGetTicket(i);
      if(ticket == 0 || !PositionSelectByTicket(ticket))
         continue;
      if(!IsOurPosition())
         continue;

      long type = PositionGetInteger(POSITION_TYPE);
      return type == POSITION_TYPE_SELL ? SIDE_SELL : SIDE_BUY;
   }

   for(int i = OrdersTotal() - 1; i >= 0; --i)
   {
      ulong ticket = OrderGetTicket(i);
      if(ticket == 0 || !OrderSelect(ticket))
         continue;
      if(!IsOurOrder())
         continue;

      long type = OrderGetInteger(ORDER_TYPE);
      if(type == ORDER_TYPE_SELL_LIMIT || type == ORDER_TYPE_SELL_STOP || type == ORDER_TYPE_SELL_STOP_LIMIT)
         return SIDE_SELL;
      return SIDE_BUY;
   }

   return settings.side;
}

double ExtremeEntryPrice(const TradeSide side)
{
   bool found = false;
   double extreme = 0.0;

   for(int i = PositionsTotal() - 1; i >= 0; --i)
   {
      ulong ticket = PositionGetTicket(i);
      if(ticket == 0 || !PositionSelectByTicket(ticket))
         continue;
      if(!IsOurPosition())
         continue;

      double price = PositionGetDouble(POSITION_PRICE_OPEN);
      if(!found)
      {
         extreme = price;
         found = true;
      }
      else if(side == SIDE_BUY)
      {
         extreme = MathMin(extreme, price);
      }
      else
      {
         extreme = MathMax(extreme, price);
      }
   }

   for(int i = OrdersTotal() - 1; i >= 0; --i)
   {
      ulong ticket = OrderGetTicket(i);
      if(ticket == 0 || !OrderSelect(ticket))
         continue;
      if(!IsOurOrder())
         continue;

      double price = OrderGetDouble(ORDER_PRICE_OPEN);
      if(!found)
      {
         extreme = price;
         found = true;
      }
      else if(side == SIDE_BUY)
      {
         extreme = MathMin(extreme, price);
      }
      else
      {
         extreme = MathMax(extreme, price);
      }
   }

   if(found)
      return extreme;

   MqlTick tick;
   if(!SymbolInfoTick(_Symbol, tick))
      return SymbolInfoDouble(_Symbol, SYMBOL_BID);

   return side == SIDE_BUY ? tick.ask : tick.bid;
}

double NextGridPrice(const TradeSide side)
{
   double distance = settings.grid_points * _Point;
   double base = ExtremeEntryPrice(side);
   if(side == SIDE_BUY)
      return NormalizePrice(base - distance);
   return NormalizePrice(base + distance);
}

string SideText(const TradeSide side)
{
   return side == SIDE_BUY ? "Achat" : "Vente";
}

string FormatMoney(const double value)
{
   return DoubleToString(value, 2);
}

string FormatPoints(const double value)
{
   return DoubleToString(value, 0);
}

string FormatPrice(const double value)
{
   return DoubleToString(value, (int)SymbolInfoInteger(_Symbol, SYMBOL_DIGITS));
}

//+------------------------------------------------------------------+
//| Panel object helpers                                             |
//+------------------------------------------------------------------+
void CreateRect(const string name, const int x, const int y, const int w, const int h, const color bg)
{
   if(ObjectFind(0, name) < 0)
      ObjectCreate(0, name, OBJ_RECTANGLE_LABEL, 0, 0, 0);

   ObjectSetInteger(0, name, OBJPROP_CORNER, CORNER_LEFT_UPPER);
   ObjectSetInteger(0, name, OBJPROP_XDISTANCE, x);
   ObjectSetInteger(0, name, OBJPROP_YDISTANCE, y);
   ObjectSetInteger(0, name, OBJPROP_XSIZE, w);
   ObjectSetInteger(0, name, OBJPROP_YSIZE, h);
   ObjectSetInteger(0, name, OBJPROP_BGCOLOR, bg);
   ObjectSetInteger(0, name, OBJPROP_BORDER_TYPE, BORDER_FLAT);
   ObjectSetInteger(0, name, OBJPROP_COLOR, clrDimGray);
   ObjectSetInteger(0, name, OBJPROP_BACK, false);
   ObjectSetInteger(0, name, OBJPROP_SELECTABLE, false);
   ObjectSetInteger(0, name, OBJPROP_HIDDEN, true);
}

void CreateLabel(const string name, const int x, const int y, const string text, const int size = 9, const color fg = clrGainsboro)
{
   if(ObjectFind(0, name) < 0)
      ObjectCreate(0, name, OBJ_LABEL, 0, 0, 0);

   ObjectSetInteger(0, name, OBJPROP_CORNER, CORNER_LEFT_UPPER);
   ObjectSetInteger(0, name, OBJPROP_XDISTANCE, x);
   ObjectSetInteger(0, name, OBJPROP_YDISTANCE, y);
   ObjectSetInteger(0, name, OBJPROP_COLOR, fg);
   ObjectSetInteger(0, name, OBJPROP_FONTSIZE, size);
   ObjectSetString(0, name, OBJPROP_FONT, "Segoe UI");
   ObjectSetString(0, name, OBJPROP_TEXT, text);
   ObjectSetInteger(0, name, OBJPROP_SELECTABLE, false);
   ObjectSetInteger(0, name, OBJPROP_HIDDEN, true);
}

void CreateButton(const string name, const int x, const int y, const int w, const int h, const string text, const color bg)
{
   if(ObjectFind(0, name) < 0)
      ObjectCreate(0, name, OBJ_BUTTON, 0, 0, 0);

   ObjectSetInteger(0, name, OBJPROP_CORNER, CORNER_LEFT_UPPER);
   ObjectSetInteger(0, name, OBJPROP_XDISTANCE, x);
   ObjectSetInteger(0, name, OBJPROP_YDISTANCE, y);
   ObjectSetInteger(0, name, OBJPROP_XSIZE, w);
   ObjectSetInteger(0, name, OBJPROP_YSIZE, h);
   ObjectSetInteger(0, name, OBJPROP_BGCOLOR, bg);
   ObjectSetInteger(0, name, OBJPROP_COLOR, clrWhite);
   ObjectSetInteger(0, name, OBJPROP_FONTSIZE, 9);
   ObjectSetString(0, name, OBJPROP_FONT, "Segoe UI");
   ObjectSetString(0, name, OBJPROP_TEXT, text);
   ObjectSetInteger(0, name, OBJPROP_SELECTABLE, false);
   ObjectSetInteger(0, name, OBJPROP_HIDDEN, true);
}

void CreateEdit(const string name, const int x, const int y, const int w, const int h, const string value)
{
   if(ObjectFind(0, name) < 0)
      ObjectCreate(0, name, OBJ_EDIT, 0, 0, 0);

   ObjectSetInteger(0, name, OBJPROP_CORNER, CORNER_LEFT_UPPER);
   ObjectSetInteger(0, name, OBJPROP_XDISTANCE, x);
   ObjectSetInteger(0, name, OBJPROP_YDISTANCE, y);
   ObjectSetInteger(0, name, OBJPROP_XSIZE, w);
   ObjectSetInteger(0, name, OBJPROP_YSIZE, h);
   ObjectSetInteger(0, name, OBJPROP_BGCOLOR, clrBlack);
   ObjectSetInteger(0, name, OBJPROP_COLOR, clrWhite);
   ObjectSetInteger(0, name, OBJPROP_BORDER_COLOR, clrDimGray);
   ObjectSetInteger(0, name, OBJPROP_FONTSIZE, 9);
   ObjectSetString(0, name, OBJPROP_FONT, "Consolas");
   ObjectSetString(0, name, OBJPROP_TEXT, value);
   ObjectSetInteger(0, name, OBJPROP_SELECTABLE, false);
   ObjectSetInteger(0, name, OBJPROP_HIDDEN, true);
}

void CreateCombo(const string name, const int x, const int y, const int w, const int h, const string value)
{
   CreateButton(name, x, y, w, h, value + "  v", clrDarkSlateGray);
}

void CreateSideDropdown()
{
   int direction_y = PANEL_Y + 42 + 6 * ROW_H;
   CreateButton(ObjName("SIDE_BUY"), PANEL_X + LABEL_W, direction_y + ROW_H, VALUE_W, ROW_H - 2, "Achat", clrDarkSlateGray);
   CreateButton(ObjName("SIDE_SELL"), PANEL_X + LABEL_W, direction_y + 2 * ROW_H, VALUE_W, ROW_H - 2, "Vente", clrDarkSlateGray);
}

void DeleteSideDropdown()
{
   ObjectDelete(0, ObjName("SIDE_BUY"));
   ObjectDelete(0, ObjName("SIDE_SELL"));
}

void ToggleSideDropdown()
{
   if(side_dropdown_open)
   {
      DeleteSideDropdown();
      side_dropdown_open = false;
   }
   else
   {
      CreateSideDropdown();
      side_dropdown_open = true;
   }
   ChartRedraw(0);
}

void CreateRow(const int row, const string label, const string value_id, const bool editable, const string value)
{
   int y = PANEL_Y + 42 + row * ROW_H;
   CreateLabel(ObjName("LBL_" + value_id), PANEL_X + 12, y + 4, label);
   if(editable)
      CreateEdit(ObjName("EDIT_" + value_id), PANEL_X + LABEL_W, y, VALUE_W, ROW_H - 2, value);
   else
      CreateLabel(ObjName("VAL_" + value_id), PANEL_X + LABEL_W, y + 4, value, 9, clrWhite);
}

void SetLabelText(const string suffix, const string text, const color fg = clrWhite)
{
   string name = ObjName("VAL_" + suffix);
   if(ObjectFind(0, name) >= 0)
   {
      ObjectSetString(0, name, OBJPROP_TEXT, text);
      ObjectSetInteger(0, name, OBJPROP_COLOR, fg);
   }
}

string EditText(const string suffix)
{
   string name = ObjName("EDIT_" + suffix);
   if(ObjectFind(0, name) < 0)
      return "";
   return ObjectGetString(0, name, OBJPROP_TEXT);
}

double EditDouble(const string suffix, const double fallback)
{
   string text = EditText(suffix);
   StringReplace(text, ",", ".");
   double value = StringToDouble(text);
   if(value <= 0.0)
      return fallback;
   return value;
}

int EditInt(const string suffix, const int fallback)
{
   int value = (int)MathRound(EditDouble(suffix, fallback));
   if(value <= 0)
      return fallback;
   return value;
}

void LoadSettingsFromPanel()
{
   settings.lots = NormalizeVolume(EditDouble("LOTS", settings.lots));
   settings.grid_points = EditInt("GRID", settings.grid_points);
   settings.pending_count = EditInt("PENDING", settings.pending_count);
   settings.take_profit_money = EditDouble("TP", settings.take_profit_money);
   settings.max_levels = MathMax(1, EditInt("MAXLEVELS", settings.max_levels));
   settings.max_slippage_points = MathMax(0, EditInt("SLIPPAGE", settings.max_slippage_points));
   settings.max_spread_points = MathMax(0, EditInt("SPREADMAX", settings.max_spread_points));

   ObjectSetString(0, ObjName("EDIT_LOTS"), OBJPROP_TEXT, DoubleToString(settings.lots, 2));
   ObjectSetString(0, ObjName("EDIT_GRID"), OBJPROP_TEXT, IntegerToString(settings.grid_points));
   ObjectSetString(0, ObjName("EDIT_PENDING"), OBJPROP_TEXT, IntegerToString(settings.pending_count));
   ObjectSetString(0, ObjName("EDIT_TP"), OBJPROP_TEXT, DoubleToString(settings.take_profit_money, 2));
   ObjectSetString(0, ObjName("EDIT_MAXLEVELS"), OBJPROP_TEXT, IntegerToString(settings.max_levels));
   ObjectSetString(0, ObjName("EDIT_SLIPPAGE"), OBJPROP_TEXT, IntegerToString(settings.max_slippage_points));
   ObjectSetString(0, ObjName("EDIT_SPREADMAX"), OBJPROP_TEXT, IntegerToString(settings.max_spread_points));
}

void SetStatus(const string text, const color fg = clrLightSkyBlue)
{
   last_status = text;
   string name = ObjName("STATUS");
   if(ObjectFind(0, name) >= 0)
   {
      ObjectSetString(0, name, OBJPROP_TEXT, text);
      ObjectSetInteger(0, name, OBJPROP_COLOR, fg);
   }
}

void BuildPanel()
{
   int panel_h = 42 + 19 * ROW_H + 80;
   CreateRect(ObjName("BG"), PANEL_X, PANEL_Y, PANEL_W, panel_h, (color)0x181818);
   CreateLabel(ObjName("TITLE"), PANEL_X + 12, PANEL_Y + 10, "Grid Averaging EA", 11, clrWhite);
   CreateLabel(ObjName("MAGIC"), PANEL_X + 185, PANEL_Y + 12, "Magic " + IntegerToString(InpMagicNumber), 8, clrSilver);

   CreateRow(0, "Symbole", "SYMBOL", false, _Symbol);
   CreateRow(1, "Swap achat", "SWAPBUY", false, "-");
   CreateRow(2, "Swap vente", "SWAPSELL", false, "-");
   CreateRow(3, "Spread actuel", "SPREAD", false, "-");
   CreateRow(4, "ATR strategie", "ATR", false, "-");
   CreateRow(5, "Valeur ATR", "ATRMONEY", false, "-");

   int direction_y = PANEL_Y + 42 + 6 * ROW_H;
   CreateLabel(ObjName("LBL_SIDE"), PANEL_X + 12, direction_y + 4, "Direction");
   CreateCombo(ObjName("SIDE"), PANEL_X + LABEL_W, direction_y, VALUE_W, ROW_H - 2, SideText(settings.side));

   CreateRow(7, "Lots", "LOTS", true, DoubleToString(settings.lots, 2));
   CreateRow(8, "Distance grille", "GRID", true, IntegerToString(settings.grid_points));
   CreateRow(9, "Pending simultanes", "PENDING", true, IntegerToString(settings.pending_count));
   CreateRow(10, "Take profit", "TP", true, DoubleToString(settings.take_profit_money, 2));
   CreateRow(11, "Max niveaux", "MAXLEVELS", true, IntegerToString(settings.max_levels));
   CreateRow(12, "Perte max+1", "LOSSNEXT", false, "-");
   CreateRow(13, "Prix max+1", "LOSSPRICE", false, "-");
   CreateRow(14, "Slippage max", "SLIPPAGE", true, IntegerToString(settings.max_slippage_points));
   CreateRow(15, "Spread max", "SPREADMAX", true, IntegerToString(settings.max_spread_points));
   CreateRow(16, "Profit cycle", "PROFIT", false, "-");
   CreateRow(17, "Positions / pendings", "COUNTS", false, "-");

   int button_y = PANEL_Y + 42 + 18 * ROW_H + 8;
   CreateButton(ObjName("EXECUTE"), PANEL_X + 12, button_y, 96, 26, "Executer", clrSeaGreen);
   CreateButton(ObjName("ADD"), PANEL_X + 118, button_y, 96, 26, "Ajouter", clrSteelBlue);
   CreateButton(ObjName("CLOSE"), PANEL_X + 224, button_y, 88, 26, "Close all", clrFireBrick);
   CreateLabel(ObjName("STATUS"), PANEL_X + 12, button_y + 36, "", 8, clrLightSkyBlue);

   ChartRedraw(0);
}

void DeletePanel()
{
   DeleteSideDropdown();
   for(int i = ObjectsTotal(0) - 1; i >= 0; --i)
   {
      string name = ObjectName(0, i);
      if(StringFind(name, PREFIX) == 0)
         ObjectDelete(0, name);
   }
}

//+------------------------------------------------------------------+
//| Calculations                                                     |
//+------------------------------------------------------------------+
double DailyAtr50Points()
{
   if(atr_handle == INVALID_HANDLE)
      return 0.0;

   double buffer[];
   ArraySetAsSeries(buffer, true);
   if(CopyBuffer(atr_handle, 0, 1, 1, buffer) != 1)
      return 0.0;

   if(_Point <= 0.0)
      return 0.0;
   return buffer[0] / _Point;
}

double StrategyAtrPoints()
{
   if(InpAtrStrategyMode == ATR_STRATEGY_FIXED_500)
      return 500.0;
   if(InpAtrStrategyMode == ATR_STRATEGY_FIXED_1000)
      return 1000.0;

   return DailyAtr50Points();
}

double AtrMoneyValue(const double atr_points)
{
   if(atr_points <= 0.0 || _Point <= 0.0)
      return 0.0;

   double volume = NormalizeVolume(settings.lots);
   double open_price = SymbolInfoDouble(_Symbol, SYMBOL_BID);
   MqlTick tick;
   if(SymbolInfoTick(_Symbol, tick) && tick.bid > 0.0)
      open_price = tick.bid;
   if(open_price <= 0.0)
      return 0.0;

   double close_price = NormalizePrice(open_price + atr_points * _Point);
   double profit = 0.0;
   if(!OrderCalcProfit(ORDER_TYPE_BUY, _Symbol, volume, open_price, close_price, profit))
      return 0.0;

   return MathAbs(profit);
}

void AddProjectedEntry(double &entry_prices[], double &entry_volumes[], const double price, const double volume)
{
   int size = ArraySize(entry_prices);
   ArrayResize(entry_prices, size + 1);
   ArrayResize(entry_volumes, size + 1);
   entry_prices[size] = NormalizePrice(price);
   entry_volumes[size] = NormalizeVolume(volume);
}

double ProjectionExtremePrice(const double &entry_prices[], const TradeSide side)
{
   int size = ArraySize(entry_prices);
   if(size <= 0)
      return 0.0;

   double extreme = entry_prices[0];
   for(int i = 1; i < size; ++i)
   {
      if(side == SIDE_BUY)
         extreme = MathMin(extreme, entry_prices[i]);
      else
         extreme = MathMax(extreme, entry_prices[i]);
   }

   return extreme;
}

void CollectManagedProjectionEntries(const TradeSide side, double &entry_prices[], double &entry_volumes[])
{
   ArrayResize(entry_prices, 0);
   ArrayResize(entry_volumes, 0);

   for(int i = PositionsTotal() - 1; i >= 0; --i)
   {
      ulong ticket = PositionGetTicket(i);
      if(ticket == 0 || !PositionSelectByTicket(ticket))
         continue;
      if(!IsOurPosition())
         continue;

      ENUM_POSITION_TYPE type = (ENUM_POSITION_TYPE)PositionGetInteger(POSITION_TYPE);
      if(side == SIDE_BUY && type != POSITION_TYPE_BUY)
         continue;
      if(side == SIDE_SELL && type != POSITION_TYPE_SELL)
         continue;

      double volume = PositionGetDouble(POSITION_VOLUME);
      double open_price = PositionGetDouble(POSITION_PRICE_OPEN);
      AddProjectedEntry(entry_prices, entry_volumes, open_price, volume);
   }

   for(int i = OrdersTotal() - 1; i >= 0; --i)
   {
      ulong ticket = OrderGetTicket(i);
      if(ticket == 0 || !OrderSelect(ticket))
         continue;
      if(!IsOurOrder())
         continue;

      ENUM_ORDER_TYPE type = (ENUM_ORDER_TYPE)OrderGetInteger(ORDER_TYPE);
      bool is_buy_order = type == ORDER_TYPE_BUY_LIMIT || type == ORDER_TYPE_BUY_STOP || type == ORDER_TYPE_BUY_STOP_LIMIT;
      bool is_sell_order = type == ORDER_TYPE_SELL_LIMIT || type == ORDER_TYPE_SELL_STOP || type == ORDER_TYPE_SELL_STOP_LIMIT;
      if(side == SIDE_BUY && !is_buy_order)
         continue;
      if(side == SIDE_SELL && !is_sell_order)
         continue;

      double volume = OrderGetDouble(ORDER_VOLUME_CURRENT);
      double open_price = OrderGetDouble(ORDER_PRICE_OPEN);
      AddProjectedEntry(entry_prices, entry_volumes, open_price, volume);
   }
}

LossProjection BuildLossProjection()
{
   LossProjection projection;
   projection.profit = 0.0;
   projection.target_price = 0.0;

   TradeSide side = HasManagedCycle() ? ActiveCycleSide() : settings.side;
   double distance = settings.grid_points * _Point;
   if(distance <= 0.0 || settings.max_levels <= 0)
      return projection;

   double entry_prices[];
   double entry_volumes[];
   CollectManagedProjectionEntries(side, entry_prices, entry_volumes);

   if(ArraySize(entry_prices) <= 0)
   {
      MqlTick tick;
      double start_price = SymbolInfoDouble(_Symbol, side == SIDE_BUY ? SYMBOL_ASK : SYMBOL_BID);
      if(SymbolInfoTick(_Symbol, tick))
         start_price = side == SIDE_BUY ? tick.ask : tick.bid;
      AddProjectedEntry(entry_prices, entry_volumes, start_price, settings.lots);
   }

   while(ArraySize(entry_prices) < settings.max_levels)
   {
      double extreme = ProjectionExtremePrice(entry_prices, side);
      double next_entry = side == SIDE_BUY ? extreme - distance : extreme + distance;
      AddProjectedEntry(entry_prices, entry_volumes, next_entry, settings.lots);
   }

   double final_extreme = ProjectionExtremePrice(entry_prices, side);
   double target_price = NormalizePrice(side == SIDE_BUY ? final_extreme - distance : final_extreme + distance);
   ENUM_ORDER_TYPE calc_type = side == SIDE_BUY ? ORDER_TYPE_BUY : ORDER_TYPE_SELL;
   double total_profit = 0.0;

   for(int i = 0; i < ArraySize(entry_prices); ++i)
   {
      double position_profit = 0.0;
      if(OrderCalcProfit(calc_type, _Symbol, entry_volumes[i], entry_prices[i], target_price, position_profit))
         total_profit += position_profit;
   }

   projection.profit = total_profit;
   projection.target_price = target_price;
   return projection;
}

void UpdatePanel()
{
   LoadSettingsFromPanel();

   double swap_buy = SymbolInfoDouble(_Symbol, SYMBOL_SWAP_LONG);
   double swap_sell = SymbolInfoDouble(_Symbol, SYMBOL_SWAP_SHORT);
   int spread = CurrentSpreadPoints();
   double atr_points = StrategyAtrPoints();
   double atr_money = AtrMoneyValue(atr_points);
   string account_currency = AccountInfoString(ACCOUNT_CURRENCY);
   double profit = TotalNetProfit();
   int positions = PositionsCount();
   int pendings = PendingOrdersCount();
   LossProjection loss_projection = BuildLossProjection();

   ObjectSetString(0, ObjName("SIDE"), OBJPROP_TEXT, SideText(settings.side) + "  v");
   SetLabelText("SYMBOL", _Symbol);
   SetLabelText("SWAPBUY", DoubleToString(swap_buy, 2), swap_buy >= 0.0 ? clrPaleGreen : clrLightCoral);
   SetLabelText("SWAPSELL", DoubleToString(swap_sell, 2), swap_sell >= 0.0 ? clrPaleGreen : clrLightCoral);
   SetLabelText("SPREAD", IntegerToString(spread) + " pts", IsSpreadAllowed() ? clrPaleGreen : clrLightCoral);
   SetLabelText("ATR", atr_points > 0.0 ? FormatPoints(atr_points) + " pts" : "-");
   SetLabelText("ATRMONEY", atr_money > 0.0 ? FormatMoney(atr_money) + " " + account_currency : "-");
   SetLabelText("LOSSNEXT", FormatMoney(loss_projection.profit), loss_projection.profit >= 0.0 ? clrPaleGreen : clrLightCoral);
   SetLabelText("LOSSPRICE", loss_projection.target_price > 0.0 ? FormatPrice(loss_projection.target_price) : "-");
   SetLabelText("PROFIT", FormatMoney(profit), profit >= 0.0 ? clrPaleGreen : clrLightCoral);
   SetLabelText("COUNTS", IntegerToString(positions) + " / " + IntegerToString(pendings));

   ChartRedraw(0);
}

//+------------------------------------------------------------------+
//| Trading actions                                                  |
//+------------------------------------------------------------------+
bool PrepareTradeContext()
{
   LoadSettingsFromPanel();
   trade.SetExpertMagicNumber(InpMagicNumber);
   trade.SetDeviationInPoints(settings.max_slippage_points);

   if(!TerminalInfoInteger(TERMINAL_TRADE_ALLOWED) || !MQLInfoInteger(MQL_TRADE_ALLOWED))
   {
      SetStatus("Trading non autorise dans le terminal.", clrLightCoral);
      return false;
   }

   if(!IsSpreadAllowed())
   {
      SetStatus("Spread trop eleve: " + IntegerToString(CurrentSpreadPoints()) + " pts.", clrLightCoral);
      return false;
   }

   return true;
}

bool OpenMarketPosition(const TradeSide side)
{
   if(!PrepareTradeContext())
      return false;

   double volume = NormalizeVolume(settings.lots);
   bool ok = false;
   if(side == SIDE_BUY)
      ok = trade.Buy(volume, _Symbol, 0.0, 0.0, 0.0, "Grid averaging");
   else
      ok = trade.Sell(volume, _Symbol, 0.0, 0.0, 0.0, "Grid averaging");

   if(!ok)
   {
      SetStatus("Erreur ouverture: " + IntegerToString((int)trade.ResultRetcode()) + " " + trade.ResultRetcodeDescription(), clrLightCoral);
      return false;
   }

   SetStatus("Position " + SideText(side) + " ouverte.", clrPaleGreen);
   return true;
}

bool PendingExistsAtPrice(const TradeSide side, const double price)
{
   ENUM_ORDER_TYPE expected_type = side == SIDE_BUY ? ORDER_TYPE_BUY_LIMIT : ORDER_TYPE_SELL_LIMIT;
   double tolerance = _Point * 0.5;

   for(int i = OrdersTotal() - 1; i >= 0; --i)
   {
      ulong ticket = OrderGetTicket(i);
      if(ticket == 0 || !OrderSelect(ticket))
         continue;
      if(!IsOurOrder())
         continue;

      if((ENUM_ORDER_TYPE)OrderGetInteger(ORDER_TYPE) != expected_type)
         continue;

      double order_price = OrderGetDouble(ORDER_PRICE_OPEN);
      if(MathAbs(order_price - price) <= tolerance)
         return true;
   }

   return false;
}

bool PlacePendingOrder(const TradeSide side, const double price)
{
   if(!PrepareTradeContext())
      return false;

   double volume = NormalizeVolume(settings.lots);
   bool ok = false;
   if(side == SIDE_BUY)
      ok = trade.BuyLimit(volume, price, _Symbol, 0.0, 0.0, ORDER_TIME_GTC, 0, "Grid level");
   else
      ok = trade.SellLimit(volume, price, _Symbol, 0.0, 0.0, ORDER_TIME_GTC, 0, "Grid level");

   if(!ok)
   {
      SetStatus("Erreur pending: " + IntegerToString((int)trade.ResultRetcode()) + " " + trade.ResultRetcodeDescription(), clrLightCoral);
      return false;
   }

   return true;
}

void TrimExcessPendingOrders()
{
   int positions = PositionsCount();
   int allowed_pending = MathMin(settings.pending_count, MathMax(0, settings.max_levels - positions));
   int pendings = PendingOrdersCount();
   int excess = pendings - allowed_pending;
   if(excess <= 0)
      return;

   for(int i = OrdersTotal() - 1; i >= 0 && excess > 0; --i)
   {
      ulong ticket = OrderGetTicket(i);
      if(ticket == 0 || !OrderSelect(ticket))
         continue;
      if(!IsOurOrder())
         continue;

      if(trade.OrderDelete(ticket))
         excess--;
   }
}

void MaintainPendingGrid()
{
   LoadSettingsFromPanel();
   if(!HasManagedCycle())
      return;

   TrimExcessPendingOrders();

   TradeSide side = ActiveCycleSide();
   int positions = PositionsCount();
   int pendings = PendingOrdersCount();
   int remaining_levels = settings.max_levels - positions - pendings;
   if(remaining_levels <= 0)
      return;

   int to_place = MathMin(settings.pending_count - pendings, remaining_levels);
   if(to_place <= 0)
      return;

   if(!IsSpreadAllowed())
   {
      SetStatus("Recharge stoppee: spread trop eleve.", clrLightCoral);
      return;
   }

   for(int i = 0; i < to_place; ++i)
   {
      double next_price = NextGridPrice(side);
      if(PendingExistsAtPrice(side, next_price))
         break;

      if(!PlacePendingOrder(side, next_price))
         break;
   }
}

void ExecuteCycle()
{
   LoadSettingsFromPanel();

   if(HasManagedCycle())
   {
      SetStatus("Cycle deja actif pour ce magic number.", clrLightCoral);
      return;
   }

   if(settings.max_levels < 1)
   {
      SetStatus("Max niveaux doit etre >= 1.", clrLightCoral);
      return;
   }

   if(OpenMarketPosition(settings.side))
      MaintainPendingGrid();
}

void AddManualPosition()
{
   LoadSettingsFromPanel();
   TradeSide side = HasManagedCycle() ? ActiveCycleSide() : settings.side;
   if(PositionsCount() >= settings.max_levels)
   {
      SetStatus("Max niveaux atteint.", clrLightCoral);
      return;
   }

   if(OpenMarketPosition(side))
   {
      TrimExcessPendingOrders();
      MaintainPendingGrid();
   }
}

void CloseAllManaged()
{
   if(!PrepareTradeContext())
      return;

   bool ok = true;

   for(int i = OrdersTotal() - 1; i >= 0; --i)
   {
      ulong ticket = OrderGetTicket(i);
      if(ticket == 0 || !OrderSelect(ticket))
         continue;
      if(!IsOurOrder())
         continue;

      if(!trade.OrderDelete(ticket))
      {
         ok = false;
         SetStatus("Erreur suppression ordre: " + IntegerToString((int)trade.ResultRetcode()), clrLightCoral);
      }
   }

   for(int i = PositionsTotal() - 1; i >= 0; --i)
   {
      ulong ticket = PositionGetTicket(i);
      if(ticket == 0 || !PositionSelectByTicket(ticket))
         continue;
      if(!IsOurPosition())
         continue;

      if(!trade.PositionClose(ticket, settings.max_slippage_points))
      {
         ok = false;
         SetStatus("Erreur cloture: " + IntegerToString((int)trade.ResultRetcode()) + " " + trade.ResultRetcodeDescription(), clrLightCoral);
      }
   }

   if(ok)
      SetStatus("Cycle cloture.", clrPaleGreen);
}

void CheckTakeProfit()
{
   if(PositionsCount() <= 0)
      return;

   double profit = TotalNetProfit();
   if(profit >= settings.take_profit_money)
   {
      SetStatus("TP global atteint: " + FormatMoney(profit), clrPaleGreen);
      CloseAllManaged();
   }
}

//+------------------------------------------------------------------+
//| Expert lifecycle                                                 |
//+------------------------------------------------------------------+
int OnInit()
{
   settings.side = SIDE_BUY;
   settings.lots = NormalizeVolume(InpLots);
   settings.grid_points = MathMax(1, InpGridDistancePoints);
   settings.pending_count = MathMax(0, InpPendingOrders);
   settings.take_profit_money = MathMax(0.01, InpTakeProfitMoney);
   settings.max_levels = MathMax(1, InpMaxLevels);
   settings.max_slippage_points = MathMax(0, InpMaxSlippagePoints);
   settings.max_spread_points = MathMax(0, InpMaxSpreadPoints);

   trade.SetExpertMagicNumber(InpMagicNumber);
   trade.SetDeviationInPoints(settings.max_slippage_points);

   if(InpAtrStrategyMode == ATR_STRATEGY_D1_50)
   {
      atr_handle = iATR(_Symbol, PERIOD_D1, 50);
      if(atr_handle == INVALID_HANDLE)
      {
         Print("Impossible de creer le handle ATR(50) D1.");
         return INIT_FAILED;
      }
   }

   BuildPanel();
   UpdatePanel();
   EventSetTimer(1);
   return INIT_SUCCEEDED;
}

void OnDeinit(const int reason)
{
   EventKillTimer();
   if(atr_handle != INVALID_HANDLE)
      IndicatorRelease(atr_handle);
   DeletePanel();
}

void OnTick()
{
   CheckTakeProfit();
   MaintainPendingGrid();

   datetime now = TimeCurrent();
   if(now != last_panel_update)
   {
      last_panel_update = now;
      UpdatePanel();
   }
}

void OnTimer()
{
   CheckTakeProfit();
   MaintainPendingGrid();
   UpdatePanel();
}

void OnChartEvent(const int id, const long &lparam, const double &dparam, const string &sparam)
{
   if(id != CHARTEVENT_OBJECT_CLICK && id != CHARTEVENT_OBJECT_ENDEDIT)
      return;

   if(id == CHARTEVENT_OBJECT_ENDEDIT)
   {
      LoadSettingsFromPanel();
      UpdatePanel();
      return;
   }

   if(sparam == ObjName("SIDE"))
   {
      if(!HasManagedCycle())
      {
         ToggleSideDropdown();
      }
      else
      {
         SetStatus("Direction verrouillee pendant le cycle.", clrLightCoral);
      }
      return;
   }

   if(sparam == ObjName("SIDE_BUY") || sparam == ObjName("SIDE_SELL"))
   {
      if(!HasManagedCycle())
      {
         settings.side = sparam == ObjName("SIDE_BUY") ? SIDE_BUY : SIDE_SELL;
         DeleteSideDropdown();
         side_dropdown_open = false;
         ObjectSetString(0, ObjName("SIDE"), OBJPROP_TEXT, SideText(settings.side) + "  v");
         UpdatePanel();
      }
      return;
   }

   if(sparam == ObjName("EXECUTE"))
   {
      ExecuteCycle();
      UpdatePanel();
      return;
   }

   if(sparam == ObjName("ADD"))
   {
      AddManualPosition();
      UpdatePanel();
      return;
   }

   if(sparam == ObjName("CLOSE"))
   {
      CloseAllManaged();
      UpdatePanel();
      return;
   }
}
