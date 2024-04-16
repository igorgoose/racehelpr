package com.igorgoose.racehelpr.scraper.kartchrono.model

import com.igorgoose.racehelpr.scraper.kartchrono.dispenser.DispenserMode

data class ApplyConfigurationRequest(
    val offset: Long? = null,
    val label: String? = null,
    val mode: DispenserMode = DispenserMode.INSTANT
)