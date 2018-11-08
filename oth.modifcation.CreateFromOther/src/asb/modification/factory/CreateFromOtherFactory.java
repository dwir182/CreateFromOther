package asb.modification.factory;

import org.compiere.grid.ICreateFrom;
import org.compiere.grid.ICreateFromFactory;
import org.compiere.model.GridTab;
import org.compiere.model.I_M_InOut;

import asb.modification.webui.apps.form.WCreateFromShipmentUI;

public class CreateFromOtherFactory implements ICreateFromFactory{

	@Override
	public ICreateFrom create(GridTab mTab) {
		String tableName = mTab.getTableName();
		if (tableName.equals(I_M_InOut.Table_Name)){
			return new WCreateFromShipmentUI(mTab);
		}
		return null;
	}

}
