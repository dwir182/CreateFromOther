package asb.modification.grid;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Vector;
import java.util.logging.Level;

import org.compiere.apps.IStatusBar;
import org.compiere.grid.CreateFrom;
import org.compiere.minigrid.IMiniTable;
import org.compiere.model.GridTab;
import org.compiere.model.MCurrency;
import org.compiere.model.MInOut;
import org.compiere.model.MInOutLine;
import org.compiere.model.MInvoice;
import org.compiere.model.MInvoiceLine;
import org.compiere.model.MInvoicePaySchedule;
import org.compiere.model.MOrder;
import org.compiere.model.MOrderLine;
import org.compiere.model.MOrderPaySchedule;
import org.compiere.model.MProduct;
import org.compiere.model.MRMA;
import org.compiere.model.MRMALine;
import org.compiere.model.MUOMConversion;
import org.compiere.model.PO;
import org.compiere.util.DB;
import org.compiere.util.DisplayType;
import org.compiere.util.Env;
import org.compiere.util.KeyNamePair;
import org.compiere.util.Msg;

/**
 *  Create Invoice Transactions from PO Orders or Receipt
 *
 *  @author Jorg Janke
 *  @version  $Id: VCreateFromInvoice.java,v 1.4 2006/07/30 00:51:28 jjanke Exp $
 *
 * @author Teo Sarca, SC ARHIPAC SERVICE SRL
 * 			<li>BF [ 1896947 ] Generate invoice from Order error
 * 			<li>BF [ 2007837 ] VCreateFrom.save() should run in trx
 */
public abstract class CreateFromInvoice extends CreateFrom
{
	/**
	 *  Protected Constructor
	 *  @param mTab MTab
	 */
	public CreateFromInvoice(GridTab mTab)
	{
		super(mTab);
		if (log.isLoggable(Level.INFO)) log.info(mTab.toString());
	}   //  VCreateFromInvoice

	/**
	 *  Dynamic Init
	 *  @return true if initialized
	 */
	public boolean dynInit() throws Exception
	{
		log.config("");
		setTitle(Msg.getElement(Env.getCtx(), "C_Invoice_ID", false) + " .. " + Msg.translate(Env.getCtx(), "CreateFrom"));

		return true;
	}   //  dynInit
	
	/**
	 * Load PBartner dependent Order/Invoice/Shipment Field.
	 * @param C_BPartner_ID
	 */
	protected ArrayList<KeyNamePair> loadDirectOrderData (int C_BPartner_ID)
	{
		String isSOTrxParam = isSOTrx ? "Y":"N";
		ArrayList<KeyNamePair> list = new ArrayList<KeyNamePair>();

		//	Display
		StringBuffer display = new StringBuffer("co.DocumentNo||' - ' ||")
				.append(DB.TO_CHAR("co.DateOrdered", DisplayType.Date, Env.getAD_Language(Env.getCtx())))
				.append("||' - '||")
				.append(DB.TO_CHAR("co.GrandTotal", DisplayType.Amount, Env.getAD_Language(Env.getCtx())));
		//
		StringBuffer sql = new StringBuffer("SELECT co.C_Order_ID,").append(display)
			.append(" FROM C_Order co "
			+ "WHERE co.C_BPartner_ID=? AND co.IsSOTrx=? AND co.DocStatus IN ('CL','CO')"
			+ " AND co.C_Order_ID IN "
				+ "(SELECT col.C_Order_ID FROM C_OrderLine col"
				+ " INNER JOIN C_Order co2 on (co2.C_Order_ID = col.C_Order_ID)"
				+ " LEFT OUTER JOIN C_InvoiceLine cil ON (col.C_OrderLine_ID = cil.C_OrderLine_ID)"
				+ " WHERE co2.C_BPartner_ID=? AND co2.IsSOTrx=? AND co2.DocStatus IN ('CL','CO')"
				+ " GROUP BY col.C_OrderLine_ID"
				+ " HAVING col.QtyOrdered - COALESCE(SUM(COALESCE(cil.QtyActual,0)),0) > 0");
			sql.append(") ORDER BY co.DateOrdered");
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try
		{
			pstmt = DB.prepareStatement(sql.toString(), null);
			pstmt.setInt(1, C_BPartner_ID);
			pstmt.setString(2, isSOTrxParam);
			pstmt.setInt(3, C_BPartner_ID);
			pstmt.setString(4, isSOTrxParam);
			rs = pstmt.executeQuery();
			while (rs.next())
			{
				list.add(new KeyNamePair(rs.getInt(1), rs.getString(2)));
			}
		}
		catch (SQLException e)
		{
			log.log(Level.SEVERE, sql.toString(), e);
		}
		finally
		{
			DB.close(rs, pstmt);
			rs = null;
			pstmt = null;
		}

		return list;
	}

	/**
	 * Load PBartner dependent Order/Invoice/Shipment Field.
	 * @param C_BPartner_ID
	 */
	protected ArrayList<KeyNamePair> loadShipmentData (int C_BPartner_ID)
	{
		String isSOTrxParam = isSOTrx ? "Y":"N";
		ArrayList<KeyNamePair> list = new ArrayList<KeyNamePair>();

		//	Display
		StringBuffer display = new StringBuffer("s.DocumentNo||' - '||")
			.append(DB.TO_CHAR("s.MovementDate", DisplayType.Date, Env.getAD_Language(Env.getCtx())));
		//
		StringBuffer sql = new StringBuffer("SELECT s.M_InOut_ID,").append(display)
			.append(" FROM M_InOut s "
			+ "WHERE s.C_BPartner_ID=? AND s.IsSOTrx=? AND s.DocStatus IN ('CL','CO')"
			+ " AND s.M_InOut_ID IN "
				+ "(SELECT sl.M_InOut_ID FROM M_InOutLine sl");
			if(!isSOTrx)
				sql.append(" LEFT OUTER JOIN M_MatchInv mi ON (sl.M_InOutLine_ID=mi.M_InOutLine_ID) "
					+ " JOIN M_InOut s2 ON (sl.M_InOut_ID=s2.M_InOut_ID) "
					+ " WHERE s2.C_BPartner_ID=? AND s2.IsSOTrx=? AND s2.DocStatus IN ('CL','CO') "
					+ " GROUP BY sl.M_InOut_ID,sl.MovementQty,mi.M_InOutLine_ID"
					+ " HAVING (sl.MovementQty<>SUM(mi.Qty) AND mi.M_InOutLine_ID IS NOT NULL)"
					+ " OR mi.M_InOutLine_ID IS NULL ");
			else
				sql.append(" INNER JOIN M_InOut s2 ON (sl.M_InOut_ID=s2.M_InOut_ID)"
					+ " LEFT JOIN C_InvoiceLine il ON sl.M_InOutLine_ID = il.M_InOutLine_ID"
					+ " WHERE s2.C_BPartner_ID=? AND s2.IsSOTrx=? AND s2.DocStatus IN ('CL','CO')"
					+ " GROUP BY sl.M_InOutLine_ID"
					+ " HAVING sl.MovementQty - sum(COALESCE(il.QtyInvoiced,0)) > 0");
			sql.append(") ORDER BY s.MovementDate");
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try
		{
			pstmt = DB.prepareStatement(sql.toString(), null);
			pstmt.setInt(1, C_BPartner_ID);
			pstmt.setString(2, isSOTrxParam);
			pstmt.setInt(3, C_BPartner_ID);
			pstmt.setString(4, isSOTrxParam);
			rs = pstmt.executeQuery();
			while (rs.next())
			{
				list.add(new KeyNamePair(rs.getInt(1), rs.getString(2)));
			}
		}
		catch (SQLException e)
		{
			log.log(Level.SEVERE, sql.toString(), e);
		}
		finally
		{
			DB.close(rs, pstmt);
			rs = null;
			pstmt = null;
		}

		return list;
	}

	/**
	 *  Load PBartner dependent Order/Invoice/Shipment Field.
	 *  @param C_BPartner_ID BPartner
	 */
	protected ArrayList<KeyNamePair> loadRMAData(int C_BPartner_ID) {
		ArrayList<KeyNamePair> list = new ArrayList<KeyNamePair>();

		String sqlStmt = "SELECT r.M_RMA_ID, r.DocumentNo || '-' || r.Amt from M_RMA r "
				+ "WHERE ISSOTRX='N' AND r.DocStatus in ('CO', 'CL') "
				+ "AND r.C_BPartner_ID=? "
				+ "AND NOT EXISTS (SELECT * FROM C_Invoice inv "
				+ "WHERE inv.M_RMA_ID=r.M_RMA_ID AND inv.DocStatus IN ('CO', 'CL'))";

		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try {
			pstmt = DB.prepareStatement(sqlStmt, null);
			pstmt.setInt(1, C_BPartner_ID);
			rs = pstmt.executeQuery();
			while (rs.next()) {
				list.add(new KeyNamePair(rs.getInt(1), rs.getString(2)));
			}
		} catch (SQLException e) {
			log.log(Level.SEVERE, sqlStmt.toString(), e);
		} finally{
			DB.close(rs, pstmt);
			rs = null;
			pstmt = null;
		}

		return list;
	}
	
	/**
	 *  Load Data - Invoiced Base Order Modification
	 *  @param C_Order_ID C_Order
	 */
	protected Vector<Vector<Object>> getDirectOrderData(int C_Order_ID)
	{
		if (log.isLoggable(Level.CONFIG)) log.config("C_Order_ID=" + C_Order_ID);
		p_order = new MOrder(Env.getCtx(), C_Order_ID, null);
		//
		Vector<Vector<Object>> data = new Vector<Vector<Object>>();
		StringBuilder sql = new StringBuilder("SELECT ");	
		sql.append("col.QtyOrdered - coalesce(sum(coalesce(cil.qtyactual,0)),0),"); 					//	1.Qty
		sql.append(" col.QtyEntered/col.QtyOrdered,"	//	2.Multiplier
			+ " col.QtyOrdered,"	// 3.QtyOrder
			+ " coalesce(sum(coalesce(cil.qtyactual,0)),0)," // 4.QtyActual
			+ "	(SELECT COALESCE(SUM(COALESCE(mil.MovementQty,0)),0) FROM M_InOutLine mil, M_InOut mi Where mil.M_InOut_id = mi.M_InOut_ID AND mil.C_OrderLine_ID = Col.C_OrderLine_ID)," //5.MovementQty
			+ " round((coalesce(sum(coalesce(cil.qtyactual,0)),0) / col.qtyordered * 100), 3)," //6.Invoicepercentage
			+ " col.monthrent - coalesce(sum(coalesce(cil.monthrent,0)),0)," //7.Monthrent
			+ " col.C_UOM_ID, COALESCE(uom.UOMSymbol, uom.Name),"			//  8..9
			+ " coalesce(col.M_Product_ID,0), coalesce(p.Name, c.Name), po.VendorProductNo, col.C_OrderLine_ID, col.Line "        //  10..14
			+ " FROM C_OrderLine col "	//COALESCE(l.M_Product_ID,0),COALESCE(p.Name,c.Name)
			);
		if (Env.isBaseLanguage(Env.getCtx(), "C_UOM"))
			sql.append(" LEFT OUTER JOIN C_UOM uom ON (col.C_UOM_ID=uom.C_UOM_ID)");
		else
			sql.append(" LEFT OUTER JOIN C_UOM_Trl uom ON (col.C_UOM_ID=uom.C_UOM_ID AND uom.AD_Language='")
				.append(Env.getAD_Language(Env.getCtx())).append("')");
		
		//Add Left Outer Join To Invoice
		sql.append(" LEFT OUTER JOIN C_InvoiceLine cil on col.c_orderline_id = cil.c_orderline_id")
			.append(" LEFT OUTER JOIN C_Invoice ci on cil.c_invoice_id = ci.c_invoice_id and ci.DocStatus Not In ('DR','RE','VO','IN')");
		
		sql.append(" LEFT OUTER JOIN M_Product p ON (col.M_Product_ID=p.M_Product_ID)"
				+ "LEFT OUTER JOIN C_Charge c ON (col.C_Charge_ID=c.C_Charge_ID)")
			.append(" INNER JOIN C_Order co ON (col.C_Order_ID=co.C_Order_ID)");
		sql.append(" LEFT OUTER JOIN M_Product_PO po ON (col.M_Product_ID = po.M_Product_ID AND co.C_BPartner_ID = po.C_BPartner_ID)")
			
			.append(" WHERE col.C_Order_ID=? ")
			.append("GROUP BY col.QtyOrdered, col.QtyEntered/col.QtyOrdered, "
				+ "col.C_UOM_ID, COALESCE(uom.UOMSymbol, uom.Name), "
				+ "col.M_Product_ID, p.Name, c.Name, po.VendorProductNo, col.C_OrderLine_ID, col.Line ");
		sql.append(" HAVING col.QtyOrdered-(SELECT COALESCE(SUM(COALESCE(cil.QtyActual,0)),0) "
				+ "FROM C_InvoiceLine cil, C_Invoice ci where cil.C_Invoice_ID = ci.C_Invoice_ID and col.C_OrderLine_ID = cil.C_OrderLine_ID) > 0 ");
		sql.append("ORDER BY col.Line");
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try
		{
			pstmt = DB.prepareStatement(sql.toString(), null);
			pstmt.setInt(1, C_Order_ID);
			rs = pstmt.executeQuery();
			while (rs.next())
			{
				Vector<Object> line = new Vector<Object>(13);
				line.add(new Boolean(false));           //  0-Selection
				BigDecimal qty = rs.getBigDecimal(1);
				BigDecimal multiplier = rs.getBigDecimal(2);
				BigDecimal qtyOrder = rs.getBigDecimal(3);
				BigDecimal qtyInvoiced = rs.getBigDecimal(4);
				BigDecimal qtyMove = rs.getBigDecimal(5);
				BigDecimal invPercentage = rs.getBigDecimal(6);
				BigDecimal Monthrent = rs.getBigDecimal(7);
				BigDecimal qtyEnter = qty.multiply(multiplier);
				line.add(qtyEnter);				//1-Qty
				line.add(Env.ZERO);				//2-Percent
				KeyNamePair pp = new KeyNamePair(rs.getInt(8), rs.getString(9).trim());
				line.add(pp);                           //  3-UOM
				pp = new KeyNamePair(rs.getInt(10), rs.getString(11));
				line.add(pp);                           //  4-Product
				line.add(qtyOrder); 		//5-QtyOrdered
				line.add(qtyMove);		//6-QtyMovement
				line.add(qtyInvoiced);			//7-QtyInvoiced
				line.add(invPercentage);	//8-InvoicePercentage
				line.add(Monthrent);		//9-MonthRent
				line.add(rs.getString(12));				// 10-VendorProductNo
				pp = new KeyNamePair(rs.getInt(13), rs.getString(14));
				line.add(pp);                           //  11-OrderLine
				line.add(null);                         //  12-Ship
				line.add(null);                         //  13-Invoice
				data.add(line);			}
		}
		catch (SQLException e)
		{
			log.log(Level.SEVERE, sql.toString(), e);
		}
		finally
		{
			DB.close(rs, pstmt);
			rs = null;
			pstmt = null;
		}

		return data;
	}
	
	/**
	 *  Load Data - Shipment not invoiced
	 *  @param M_InOut_ID InOut
	 */
	protected Vector<Vector<Object>> getShipmentData(int M_InOut_ID)
	{
		if (log.isLoggable(Level.CONFIG)) log.config("M_InOut_ID=" + M_InOut_ID);
		MInOut inout = new MInOut(Env.getCtx(), M_InOut_ID, null);
		p_order = null;
		if (inout.getC_Order_ID() != 0)
			p_order = new MOrder (Env.getCtx(), inout.getC_Order_ID(), null);

		m_rma = null;
		if (inout.getM_RMA_ID() != 0)
			m_rma = new MRMA (Env.getCtx(), inout.getM_RMA_ID(), null);

		//
		Vector<Vector<Object>> data = new Vector<Vector<Object>>();
		StringBuilder sql = new StringBuilder("SELECT ");	//	QtyEntered
		if(!isSOTrx)
			sql.append("l.MovementQty-SUM(COALESCE(mi.Qty, 0)),");
		else
			sql.append("l.MovementQty-SUM(COALESCE(il.QtyInvoiced,0)),");
		sql.append(" l.QtyEntered/l.MovementQty,"			// 2- multiplier
			+ " (select col.qtyordered from c_orderline col, c_order co where col.c_order_id = co.c_order_id and col.c_orderline_id = l.c_orderline_id)," // 3-QtyOrder
			+ " (select coalesce(sum(coalesce(cil.qtyinvoiced,0)),0) from c_invoiceline cil, c_invoice ci where ci.c_invoice_id = cil.c_invoice_id and l.m_inoutline_id = cil.m_inoutline_id and ci.docstatus not in ('RE','VO','IN'))," // 4.QtyInvoiced
			+ "	(SELECT COALESCE(SUM(COALESCE(mil.MovementQty,0)),0) FROM M_InOutLine mil, M_InOut mi Where mil.M_InOut_id = mi.M_InOut_ID AND mil.C_OrderLine_ID = l.C_OrderLine_ID and mi.docstatus not in ('RE','VO','IN'))," //5.MovementQty
			+ " l.C_UOM_ID, COALESCE(uom.UOMSymbol, uom.Name),"			//  6..7
			+ " l.M_Product_ID, p.Name, po.VendorProductNo, l.M_InOutLine_ID, l.Line,"        //  8..12
			+ " l.C_OrderLine_ID " //  13
			+ " FROM M_InOutLine l "
			);
		if (Env.isBaseLanguage(Env.getCtx(), "C_UOM"))
			sql.append(" LEFT OUTER JOIN C_UOM uom ON (l.C_UOM_ID=uom.C_UOM_ID)");
		else
			sql.append(" LEFT OUTER JOIN C_UOM_Trl uom ON (l.C_UOM_ID=uom.C_UOM_ID AND uom.AD_Language='")
				.append(Env.getAD_Language(Env.getCtx())).append("')");

		sql.append(" LEFT OUTER JOIN M_Product p ON (l.M_Product_ID=p.M_Product_ID)")
			.append(" INNER JOIN M_InOut io ON (l.M_InOut_ID=io.M_InOut_ID)");
		if(!isSOTrx)
			sql.append(" LEFT OUTER JOIN M_MatchInv mi ON (l.M_InOutLine_ID=mi.M_InOutLine_ID)");
		else
			sql.append(" LEFT JOIN C_InvoiceLine il ON l.M_InOutLine_ID = il.M_InOutLine_ID");
		sql.append(" LEFT OUTER JOIN M_Product_PO po ON (l.M_Product_ID = po.M_Product_ID AND io.C_BPartner_ID = po.C_BPartner_ID)")

			.append(" WHERE l.M_InOut_ID=? AND l.MovementQty<>0 ")
			.append("GROUP BY l.MovementQty, l.QtyEntered/l.MovementQty, "
				+ "l.C_UOM_ID, COALESCE(uom.UOMSymbol, uom.Name), "
				+ "l.M_Product_ID, p.Name, po.VendorProductNo, l.M_InOutLine_ID, l.Line, l.C_OrderLine_ID ");
		if(!isSOTrx)
			sql.append(" HAVING l.MovementQty-SUM(COALESCE(mi.Qty, 0)) <>0");
		else
			sql.append(" HAVING l.MovementQty-SUM(COALESCE(il.QtyInvoiced,0)) <>0");
		sql.append("ORDER BY l.Line");
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try
		{
			pstmt = DB.prepareStatement(sql.toString(), null);
			pstmt.setInt(1, M_InOut_ID);
			rs = pstmt.executeQuery();
			while (rs.next())
			{
				Vector<Object> line = new Vector<Object>(13);
				line.add(new Boolean(false));           //  0-Selection
				BigDecimal qtyMovement = rs.getBigDecimal(1);
				BigDecimal multiplier = rs.getBigDecimal(2);
				BigDecimal qtyOrder = rs.getBigDecimal(3);
				BigDecimal qtyInv = rs.getBigDecimal(4);
				BigDecimal qtyMove = rs.getBigDecimal(5);
				BigDecimal qtyEntered = qtyMovement.multiply(multiplier);
				line.add(qtyEntered);  //  1-Qty
				line.add(null);		// 2-Percent
				KeyNamePair pp = new KeyNamePair(rs.getInt(6), rs.getString(7).trim());
				line.add(pp);                           //  3-UOM
				pp = new KeyNamePair(rs.getInt(8), rs.getString(9));
				line.add(pp);                           //  4-Product
				line.add(qtyOrder);	// 5-Order
				line.add(qtyMove); // 6-Move
				line.add(qtyInv); // 7-Inv
				line.add(null);	// 8-Percentage
				line.add(null); // 9-MonthRent
				line.add(rs.getString(10));				// 10-VendorProductNo
				int C_OrderLine_ID = rs.getInt(13);
				if (rs.wasNull())
					line.add(null);                     //  11-Order
				else
					line.add(new KeyNamePair(C_OrderLine_ID,"."));
				pp = new KeyNamePair(rs.getInt(11), rs.getString(12));
				line.add(pp);                           //  12-Ship
				line.add(null);                     	//  13-RMA
				data.add(line);
			}
		}
		catch (SQLException e)
		{
			log.log(Level.SEVERE, sql.toString(), e);
		}
		finally
		{
			DB.close(rs, pstmt);
			rs = null;
			pstmt = null;
		}

		return data;
	}   //  loadShipment

	/**
	 * Load RMA details
	 * @param M_RMA_ID RMA
	 */
	protected Vector<Vector<Object>> getRMAData(int M_RMA_ID)
	{
	    p_order = null;

//	    MRMA m_rma = new MRMA(Env.getCtx(), M_RMA_ID, null);

	    Vector<Vector<Object>> data = new Vector<Vector<Object>>();
	    StringBuilder sqlStmt = new StringBuilder();
	    sqlStmt.append("SELECT rl.M_RMALine_ID, rl.line, rl.Qty - COALESCE(rl.QtyInvoiced, 0), iol.M_Product_ID, p.Name, uom.C_UOM_ID, COALESCE(uom.UOMSymbol,uom.Name) ");
	    sqlStmt.append("FROM M_RMALine rl INNER JOIN M_InOutLine iol ON rl.M_InOutLine_ID=iol.M_InOutLine_ID ");

	    if (Env.isBaseLanguage(Env.getCtx(), "C_UOM"))
        {
	        sqlStmt.append("LEFT OUTER JOIN C_UOM uom ON (uom.C_UOM_ID=iol.C_UOM_ID) ");
        }
	    else
        {
	        sqlStmt.append("LEFT OUTER JOIN C_UOM_Trl uom ON (uom.C_UOM_ID=iol.C_UOM_ID AND uom.AD_Language='");
	        sqlStmt.append(Env.getAD_Language(Env.getCtx())).append("') ");
        }
	    sqlStmt.append("LEFT OUTER JOIN M_Product p ON p.M_Product_ID=iol.M_Product_ID ");
	    sqlStmt.append("WHERE rl.M_RMA_ID=? ");
	    sqlStmt.append("AND rl.M_INOUTLINE_ID IS NOT NULL");

	    sqlStmt.append(" UNION ");

	    sqlStmt.append("SELECT rl.M_RMALine_ID, rl.line, rl.Qty - rl.QtyDelivered, 0, c.Name, uom.C_UOM_ID, COALESCE(uom.UOMSymbol,uom.Name) ");
	    sqlStmt.append("FROM M_RMALine rl INNER JOIN C_Charge c ON c.C_Charge_ID = rl.C_Charge_ID ");
	    if (Env.isBaseLanguage(Env.getCtx(), "C_UOM"))
        {
	        sqlStmt.append("LEFT OUTER JOIN C_UOM uom ON (uom.C_UOM_ID=100) ");
        }
	    else
        {
	        sqlStmt.append("LEFT OUTER JOIN C_UOM_Trl uom ON (uom.C_UOM_ID=100 AND uom.AD_Language='");
	        sqlStmt.append(Env.getAD_Language(Env.getCtx())).append("') ");
        }
	    sqlStmt.append("WHERE rl.M_RMA_ID=? ");
	    sqlStmt.append("AND rl.C_Charge_ID IS NOT NULL");

	    PreparedStatement pstmt = null;
	    ResultSet rs = null;
	    try
	    {
	        pstmt = DB.prepareStatement(sqlStmt.toString(), null);
	        pstmt.setInt(1, M_RMA_ID);
	        pstmt.setInt(2, M_RMA_ID);
	        rs = pstmt.executeQuery();

	        while (rs.next())
            {
	            Vector<Object> line = new Vector<Object>(13);
	            line.add(new Boolean(false));   // 0-Selection
	            line.add(rs.getBigDecimal(3));  // 1-Qty
	            line.add(null);	//	2- Percent
	            KeyNamePair pp = new KeyNamePair(rs.getInt(6), rs.getString(7));
	            line.add(pp); // 3-UOM
	            pp = new KeyNamePair(rs.getInt(4), rs.getString(5));
	            line.add(pp); // 4-Product
	            line.add(null);		//5-QtyOrd
	            line.add(null); 	//6-Qtyinv
	            line.add(null);		//7-QtyMove
	            line.add(null);		//8-InvPer
	            line.add(null);		//9-MontRent
	            line.add(null); //10-Vendor Product No
	            line.add(null); //11-Order
	            pp = new KeyNamePair(rs.getInt(1), rs.getString(2));
	            line.add(null);   //12-Ship
	            line.add(pp);   //13-RMA
	            data.add(line);
            }
	    }
	    catch (Exception ex)
	    {
	        log.log(Level.SEVERE, sqlStmt.toString(), ex);
	    }
	    finally
	    {
	    	DB.close(rs, pstmt);
	    	rs = null; pstmt = null;
	    }

	    return data;
	}
	/**
	 *  Load Data - Order
	 *  @param C_Order_ID Order
	 *  @param forInvoice true if for invoice vs. delivery qty
	 * 	Override getOrderData from CreateFrom
	 */
	@Override
	protected Vector<Vector<Object>> getOrderData (int C_Order_ID, boolean forInvoice)
	{
		/**
		 *  Selected        	- 0
		 *  Qty             	- 1
		 *  Multiplier			- 2
		 *  QtyOrder			- 3
		 *  QtyMove				- 4
		 *  QtyInvoiced			- 5
		 *  InvoicePercentage	- 6
		 *  Month Rent			- 7	
		 *  C_UOM_ID        	- 8
		 *  M_Product_ID    	- 9
		 *  VendorProductNo 	- 10
		 *  OrderLine       	- 11
		 *  ShipmentLine    	- 12
		 *  InvoiceLine     	- 13
		 */
		if (log.isLoggable(Level.CONFIG)) log.config("C_Order_ID=" + C_Order_ID);
		p_order = new MOrder (Env.getCtx(), C_Order_ID, null);

		Vector<Vector<Object>> data = new Vector<Vector<Object>>();
		StringBuilder sql = new StringBuilder("SELECT "
			+ "l.QtyOrdered-SUM(COALESCE(m.Qty,0)),"					//	1
			+ "CASE WHEN l.QtyOrdered=0 THEN 0 ELSE l.QtyEntered/l.QtyOrdered END,"	// 2
			+ " l.QtyOrdered,"	// 3
			+ " (select sum(coalesce(mil.movementqty,0)) from m_inoutline mil, m_inout mi where mil.m_inout_id = mi.m_inout_id and mil.c_orderline_id = l.c_orderline_id),"	// 4
			+ " (select sum(coalesce(cil.qtyinvoiced,0)) from c_invoiceline cil, c_invoice ci where ci.c_invoice_id = cil.c_invoice_id and cil.c_orderline_id = l.c_orderline_id),"	// 5
			+ " l.C_UOM_ID,COALESCE(uom.UOMSymbol,uom.Name),"			//	6..7
			+ " COALESCE(l.M_Product_ID,0),COALESCE(p.Name,c.Name),po.VendorProductNo,"	//	8..10
			+ " l.C_OrderLine_ID,l.Line "								//	11..12
			+ "FROM C_OrderLine l"
			+ " LEFT OUTER JOIN M_Product_PO po ON (l.M_Product_ID = po.M_Product_ID AND l.C_BPartner_ID = po.C_BPartner_ID) "
			+ " LEFT OUTER JOIN M_MatchPO m ON (l.C_OrderLine_ID=m.C_OrderLine_ID AND ");
		sql.append(forInvoice ? "m.C_InvoiceLine_ID" : "m.M_InOutLine_ID");
		sql.append(" IS NOT NULL)")
			.append(" LEFT OUTER JOIN M_Product p ON (l.M_Product_ID=p.M_Product_ID)"
			+ " LEFT OUTER JOIN C_Charge c ON (l.C_Charge_ID=c.C_Charge_ID)");
		if (Env.isBaseLanguage(Env.getCtx(), "C_UOM"))
			sql.append(" LEFT OUTER JOIN C_UOM uom ON (l.C_UOM_ID=uom.C_UOM_ID)");
		else
			sql.append(" LEFT OUTER JOIN C_UOM_Trl uom ON (l.C_UOM_ID=uom.C_UOM_ID AND uom.AD_Language='")
				.append(Env.getAD_Language(Env.getCtx())).append("')");
		//
		sql.append(" WHERE l.C_Order_ID=? "			//	#1
			+ "GROUP BY l.QtyOrdered,CASE WHEN l.QtyOrdered=0 THEN 0 ELSE l.QtyEntered/l.QtyOrdered END, "
			+ "l.C_UOM_ID,COALESCE(uom.UOMSymbol,uom.Name),po.VendorProductNo, "
				+ "l.M_Product_ID,COALESCE(p.Name,c.Name), l.Line,l.C_OrderLine_ID "
			+ "ORDER BY l.Line");
		//
		if (log.isLoggable(Level.FINER)) log.finer(sql.toString());
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try
		{
			pstmt = DB.prepareStatement(sql.toString(), null);
			pstmt.setInt(1, C_Order_ID);
			rs = pstmt.executeQuery();
			while (rs.next())
			{
				Vector<Object> line = new Vector<Object>(13);
				line.add(new Boolean(false));           //  0-Selection
				BigDecimal qtyOrdered = rs.getBigDecimal(1);
				BigDecimal multiplier = rs.getBigDecimal(2);
				BigDecimal qtyOrder = rs.getBigDecimal(3);
				BigDecimal qtyMove = rs.getBigDecimal(4);
				BigDecimal qtyInv = rs.getBigDecimal(5);
				BigDecimal qtyEntered = qtyOrdered.multiply(multiplier);
				line.add(qtyEntered);                   //  1-Qty
				line.add(null);							//	2-Percent
				KeyNamePair pp = new KeyNamePair(rs.getInt(6), rs.getString(7).trim());
				line.add(pp);                           //  3-UOM
				pp = new KeyNamePair(rs.getInt(8), rs.getString(9));
				line.add(pp);                           //  4-Product
				line.add(qtyOrder);						// 	5-QtyOrder
				line.add(qtyMove);						//	6-QtyMove
				line.add(qtyInv);						//	7-QtyInv
				line.add(null);							//	8-InvPerc
				line.add(null);							//	9-MontRent
				line.add(rs.getString(10));				// 	10-VendorProductNo
				pp = new KeyNamePair(rs.getInt(11), rs.getString(12));
				line.add(pp);                           //  11-OrderLine
				line.add(null);                         //  12-Ship
				line.add(null);                         //  13-Invoice
				data.add(line);
			}
		}
		catch (Exception e)
		{
			log.log(Level.SEVERE, sql.toString(), e);
		}
		finally
		{
			DB.close(rs, pstmt);
			rs = null; pstmt = null;
		}

		return data;
	}   //  LoadOrder

	/**
	 *  List number of rows selected
	 */
	public void info(IMiniTable miniTable, IStatusBar statusBar)
	{

	}   //  infoInvoice

	protected void configureMiniTable (IMiniTable miniTable)
	{
		miniTable.setColumnClass(0, Boolean.class, false);      //  0-Selection
		miniTable.setColumnClass(1, BigDecimal.class, false);        //  1-Qty
		miniTable.setColumnClass(2, BigDecimal.class, false);		 //	 2-Percent
		miniTable.setColumnClass(3, String.class, true);        //  2-Uom
		miniTable.setColumnClass(4, String.class, true);        //  3-Product
		miniTable.setColumnClass(5, BigDecimal.class, true);        //  4-QtyOrder
		miniTable.setColumnClass(6, BigDecimal.class, true);        //  5-QtyMove
		miniTable.setColumnClass(7, BigDecimal.class, true);        //  6-QtyInv
		miniTable.setColumnClass(8, BigDecimal.class, true);        //  7-Percentage
		miniTable.setColumnClass(9, BigDecimal.class, true);		//	8-MonthRent
		miniTable.setColumnClass(10, String.class, true);        //  9-VendorProductNo
		miniTable.setColumnClass(11, String.class, true);        //  10-Order
		miniTable.setColumnClass(12, String.class, true);       // 	11-Ship
		miniTable.setColumnClass(13, String.class, true);       // 	12-Invoice
		//  Table UI
		miniTable.autoSize();
	}

	/**
	 *  Save - Create Invoice Lines
	 *  @return true if saved
	 */
	public boolean save(IMiniTable miniTable, String trxName)
	{
		//  Invoice
		int C_Invoice_ID = ((Integer)getGridTab().getValue("C_Invoice_ID")).intValue();
		MInvoice invoice = new MInvoice (Env.getCtx(), C_Invoice_ID, trxName);
		if (log.isLoggable(Level.CONFIG)) log.config(invoice.toString());

		if (p_order != null)
		{
			invoice.setOrder(p_order);	//	overwrite header values
			invoice.saveEx();
		}

		if (m_rma != null)
		{
			invoice.setM_RMA_ID(m_rma.getM_RMA_ID());
			invoice.saveEx();
		}

//		MInOut inout = null;
//		if (m_M_InOut_ID > 0)
//		{
//			inout = new MInOut(Env.getCtx(), m_M_InOut_ID, trxName);
//		}
//		if (inout != null && inout.getM_InOut_ID() != 0
//			&& inout.getC_Invoice_ID() == 0)	//	only first time
//		{
//			inout.setC_Invoice_ID(C_Invoice_ID);
//			inout.saveEx();
//		}

		//  Lines
		for (int i = 0; i < miniTable.getRowCount(); i++)
		{
			if (((Boolean)miniTable.getValueAt(i, 0)).booleanValue())
			{
				MProduct product = null;
				//  variable values
				BigDecimal QtyEntered = (BigDecimal)miniTable.getValueAt(i, 1);              //  1-Qty
				BigDecimal InvoicePercentage = (BigDecimal)miniTable.getValueAt(i, 2);		// 2-InvoicePercentage

				KeyNamePair pp = (KeyNamePair)miniTable.getValueAt(i, 3);   //  3-UOM
				int C_UOM_ID = pp.getKey();
				//
				pp = (KeyNamePair)miniTable.getValueAt(i, 4);               //  4-Product
				int M_Product_ID = 0;
				if (pp != null)
					M_Product_ID = pp.getKey();
				//
				int C_OrderLine_ID = 0;
				pp = (KeyNamePair)miniTable.getValueAt(i, 11);               //  5-OrderLine
				if (pp != null)
					C_OrderLine_ID = pp.getKey();
				int M_InOutLine_ID = 0;
				pp = (KeyNamePair)miniTable.getValueAt(i, 12);               //  6-Shipment
				if (pp != null)
					M_InOutLine_ID = pp.getKey();
				//
				int M_RMALine_ID = 0;
				pp = (KeyNamePair)miniTable.getValueAt(i, 13);               //  7-RMALine
				if (pp != null)
					M_RMALine_ID = pp.getKey();

				//	Precision of Qty UOM
				int precision = 2;
				if (M_Product_ID != 0)
				{
					product = MProduct.get(Env.getCtx(), M_Product_ID);
					precision = product.getUOMPrecision();
				}
				QtyEntered = QtyEntered.setScale(precision, BigDecimal.ROUND_HALF_DOWN);
				//
				if (log.isLoggable(Level.FINE)) log.fine("Line QtyEntered=" + QtyEntered
					+ ", Product_ID=" + M_Product_ID
					+ ", OrderLine_ID=" + C_OrderLine_ID + ", InOutLine_ID=" + M_InOutLine_ID);

				//	Create new Invoice Line
				MInvoiceLine invoiceLine = new MInvoiceLine (invoice);
				invoiceLine.setM_Product_ID(M_Product_ID, C_UOM_ID);	//	Line UOM
				invoiceLine.setQty(QtyEntered);							//	Invoiced/Entered
				invoiceLine.setInvoicePercentage(InvoicePercentage); 	//	Invoice Percentage
				BigDecimal QtyInvoiced = null;
				BigDecimal QtyActual = null;
				if (M_Product_ID > 0 && product.getC_UOM_ID() != C_UOM_ID) {
					QtyInvoiced = MUOMConversion.convertProductFrom(Env.getCtx(), M_Product_ID, C_UOM_ID, QtyEntered);
				}
				if (QtyInvoiced == null)
					QtyInvoiced = QtyEntered;
				invoiceLine.setQtyInvoiced(QtyInvoiced);
				
				if (InvoicePercentage == null || InvoicePercentage.compareTo(Env.ZERO) == 0)
					QtyActual = QtyInvoiced;
				if (InvoicePercentage.compareTo(Env.ZERO) != 0)
					QtyActual = (InvoicePercentage.divide(new BigDecimal(100))).multiply(QtyEntered);
				
				invoiceLine.setQtyActual(QtyActual);
				

				//  Info
				MOrderLine orderLine = null;
				if (C_OrderLine_ID != 0)
					orderLine = new MOrderLine (Env.getCtx(), C_OrderLine_ID, trxName);
				//
				MRMALine rmaLine = null;
				if (M_RMALine_ID > 0)
					rmaLine = new MRMALine (Env.getCtx(), M_RMALine_ID, null);
				//
				MInOutLine inoutLine = null;
				if (M_InOutLine_ID != 0)
				{
					inoutLine = new MInOutLine (Env.getCtx(), M_InOutLine_ID, trxName);
					if (orderLine == null && inoutLine.getC_OrderLine_ID() != 0)
					{
						C_OrderLine_ID = inoutLine.getC_OrderLine_ID();
						orderLine = new MOrderLine (Env.getCtx(), C_OrderLine_ID, trxName);
					}
				}
				else if (C_OrderLine_ID > 0)
				{
					String whereClause = "EXISTS (SELECT 1 FROM M_InOut io WHERE io.M_InOut_ID=M_InOutLine.M_InOut_ID AND io.DocStatus IN ('CO','CL'))";
					MInOutLine[] lines = MInOutLine.getOfOrderLine(Env.getCtx(),
						C_OrderLine_ID, whereClause, trxName);
					if (log.isLoggable(Level.FINE)) log.fine ("Receipt Lines with OrderLine = #" + lines.length);
					if (lines.length > 0)
					{
						for (int j = 0; j < lines.length; j++)
						{
							MInOutLine line = lines[j];
							if (line.getQtyEntered().compareTo(QtyEntered) == 0)
							{
								inoutLine = line;
								M_InOutLine_ID = inoutLine.getM_InOutLine_ID();
								break;
							}
						}
//						if (inoutLine == null)
//						{
//							inoutLine = lines[0];	//	first as default
//							M_InOutLine_ID = inoutLine.getM_InOutLine_ID();
//						}
					}
				}
				else if (M_RMALine_ID != 0)
				{
					String whereClause = "EXISTS (SELECT 1 FROM M_InOut io WHERE io.M_InOut_ID=M_InOutLine.M_InOut_ID AND io.DocStatus IN ('CO','CL'))";
					MInOutLine[] lines = MInOutLine.getOfRMALine(Env.getCtx(), M_RMALine_ID, whereClause, null);
					if (log.isLoggable(Level.FINE)) log.fine ("Receipt Lines with RMALine = #" + lines.length);
					if (lines.length > 0)
					{
						for (int j = 0; j < lines.length; j++)
						{
							MInOutLine line = lines[j];
							if (rmaLine.getQty().compareTo(QtyEntered) == 0)
							{
								inoutLine = line;
								M_InOutLine_ID = inoutLine.getM_InOutLine_ID();
								break;
							}
						}
						if (rmaLine == null)
						{
							inoutLine = lines[0];	//	first as default
							M_InOutLine_ID = inoutLine.getM_InOutLine_ID();
						}
					}

				}
				//	get Ship info
				
				//	Shipment Info
				if (inoutLine != null)
				{
					invoiceLine.setShipLine(inoutLine);		//	overwrites
				}
				else {
					log.fine("No Receipt Line");
					//	Order Info
					if (orderLine != null)
					{
						invoiceLine.setOrderLine(orderLine);	//	overwrites
					}
					else
					{
						log.fine("No Order Line");
						invoiceLine.setPrice();
						invoiceLine.setTax();
					}

					//RMA Info
					if (rmaLine != null)
					{
						invoiceLine.setRMALine(rmaLine);		//	overwrites
					}
					else
						log.fine("No RMA Line");
				}
				invoiceLine.saveEx();
			}   //   if selected
		}   //  for all rows

		if (p_order != null) {
			invoice.setPaymentRule(p_order.getPaymentRule());
			invoice.setC_PaymentTerm_ID(p_order.getC_PaymentTerm_ID());
			invoice.saveEx();
			invoice.load(invoice.get_TrxName()); // refresh from DB
			// copy payment schedule from order if invoice doesn't have a current payment schedule
			MOrderPaySchedule[] opss = MOrderPaySchedule.getOrderPaySchedule(invoice.getCtx(), p_order.getC_Order_ID(), 0, invoice.get_TrxName());
			MInvoicePaySchedule[] ipss = MInvoicePaySchedule.getInvoicePaySchedule(invoice.getCtx(), invoice.getC_Invoice_ID(), 0, invoice.get_TrxName());
			if (ipss.length == 0 && opss.length > 0) {
				BigDecimal ogt = p_order.getGrandTotal();
				BigDecimal igt = invoice.getGrandTotal();
				BigDecimal percent = Env.ONE;
				if (ogt.compareTo(igt) != 0)
					percent = igt.divide(ogt, 10, BigDecimal.ROUND_HALF_UP);
				MCurrency cur = MCurrency.get(p_order.getCtx(), p_order.getC_Currency_ID());
				int scale = cur.getStdPrecision();
			
				for (MOrderPaySchedule ops : opss) {
					MInvoicePaySchedule ips = new MInvoicePaySchedule(invoice.getCtx(), 0, invoice.get_TrxName());
					PO.copyValues(ops, ips);
					if (percent != Env.ONE) {
						BigDecimal propDueAmt = ops.getDueAmt().multiply(percent);
						if (propDueAmt.scale() > scale)
							propDueAmt = propDueAmt.setScale(scale, BigDecimal.ROUND_HALF_UP);
						ips.setDueAmt(propDueAmt);
					}
					ips.setC_Invoice_ID(invoice.getC_Invoice_ID());
					ips.setAD_Org_ID(ops.getAD_Org_ID());
					ips.setProcessing(ops.isProcessing());
					ips.setIsActive(ops.isActive());
					ips.saveEx();
				}
				invoice.validatePaySchedule();
				invoice.saveEx();
			}
		}

		return true;
	}   //  saveInvoice

	protected Vector<String> getOISColumnNames()
	{
		//  Header Info
	    Vector<String> columnNames = new Vector<String>(13);
	    columnNames.add(Msg.getMsg(Env.getCtx(), "Select"));
	    columnNames.add(Msg.translate(Env.getCtx(), "Quantity"));
	    columnNames.add(Msg.translate(Env.getCtx(), "Percent"));
	    columnNames.add(Msg.translate(Env.getCtx(), "C_UOM_ID"));
	    columnNames.add(Msg.translate(Env.getCtx(), "M_Product_ID"));
	    columnNames.add(Msg.translate(Env.getCtx(), "QtyOrdered"));
	    columnNames.add(Msg.translate(Env.getCtx(), "MovementQty"));
	    columnNames.add(Msg.translate(Env.getCtx(), "QtyInvoiced"));
	    columnNames.add(Msg.translate(Env.getCtx(), "InvoicePercentage"));
	    columnNames.add(Msg.translate(Env.getCtx(), "MonthRent"));
	    columnNames.add(Msg.getElement(Env.getCtx(), "VendorProductNo", isSOTrx));
	    columnNames.add(Msg.getElement(Env.getCtx(), "C_Order_ID", isSOTrx));
	    columnNames.add(Msg.getElement(Env.getCtx(), "M_InOut_ID", isSOTrx));
	    columnNames.add(Msg.getElement(Env.getCtx(), "M_RMA_ID", isSOTrx));

	    return columnNames;
	}

}
