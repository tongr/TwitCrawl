package de.hpi.fgis.json;

import java.util.LinkedList;
import java.util.List;

import com.mongodb.DBObject;

/**
 * enables the execution of a {@link ITransformation} sequence
 * 
 * @author tongr
 * 
 */
public class ChainedTransformation implements ITransformation {
	private final List<ITransformation> transformations = new LinkedList<ITransformation>();

	@Override
	public DBObject transform(DBObject orig) {
		for (ITransformation transformation : transformations) {
			orig = transformation.transform(orig);
		}
		return orig;
	}

	/**
	 * adds a new transformation to the chain (performed after the previously
	 * defined steps)
	 * 
	 * @param transformation
	 *            the transformation to be added
	 * @return this instance
	 */
	public ChainedTransformation addTransformation(
			ITransformation transformation) {
		if (transformation != null) {
			this.transformations.add(transformation);
		}
		return this;
	}
}
