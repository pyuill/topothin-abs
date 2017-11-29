/*
 * Copyright (c) 2017 Peter Yuill
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution.
 * The Eclipse Public License is available at http://www.eclipse.org/legal/epl-v10.html
 */
package au.id.yuill.topothin.abs;

import org.locationtech.jts.geom.MultiPolygon;

/**
 * A holder class for admin polygons
 *
 * @version 1.0
 * @author Peter Yuill
 */
public class AdminArea {
    public String code;
    public MultiPolygon geom;
}
