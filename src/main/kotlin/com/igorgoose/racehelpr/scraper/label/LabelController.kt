package com.igorgoose.racehelpr.scraper.label

import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/labels")
class LabelController(private val labelManager: LabelManager) {

    @GetMapping
    fun getAll(): List<Label> = labelManager.getAllLabels()
}