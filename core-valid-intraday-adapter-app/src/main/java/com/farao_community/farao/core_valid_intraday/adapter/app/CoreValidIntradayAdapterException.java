/*
 * Copyright (c) 2025, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.farao_community.farao.core_valid_intraday.adapter.app;

/**
 * @author Marc Schwitzguebel {@literal <marc.schwitzguebel_external at rte-france.com>}
 */
public class CoreValidIntradayAdapterException extends RuntimeException {

    public CoreValidIntradayAdapterException(final String message, final Throwable throwable) {
        super(message, throwable);
    }

    public CoreValidIntradayAdapterException(final String message) {
        super(message);
    }
}
