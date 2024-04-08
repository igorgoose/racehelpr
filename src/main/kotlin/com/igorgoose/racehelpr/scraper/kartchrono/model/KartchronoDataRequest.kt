package com.igorgoose.racehelpr.scraper.kartchrono.model

data class KartchronoDataRequest(
    val offset: Long? = null,
    val label: String? = null,
    val realTimeMode: Boolean = false
)