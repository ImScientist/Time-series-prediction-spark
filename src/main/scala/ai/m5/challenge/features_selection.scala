package ai.m5.challenge

object features_selection {

  def getFeatures(all_features: Seq[String]): (Seq[String], Seq[String]) = {

    val features_cat_item = Seq(
      "item_id_indexed", "dept_id_indexed", "cat_id_indexed", "store_id_indexed", "state_id_indexed"
    )

    val features_cat_time = Seq()
    val features_cat_other = if (all_features.contains("snap")) Seq("snap") else Seq()
    val features_scale = if (all_features.contains("scale")) Seq("scale") else Seq()

    val features_event_name = all_features.filter(x => x.startsWith("event_name_"))
    val features_event_type = all_features.filter(x => x.startsWith("event_type_"))
    val features_sales = all_features.filter(x => x.startsWith("sales_"))
    val features_price = all_features.filter(x => x.startsWith("sell_price"))

    val features_continuous_time = Seq("year", "quarter", "month", "day_in_month", "day_in_week", "week_in_yr")

    val features_categorical =
      features_cat_item ++
        features_cat_time ++
        features_event_name ++
        features_event_type ++
        features_cat_other

    val features_continuous =
      features_continuous_time ++
        features_price ++
        features_sales ++
        features_scale

    (features_categorical, features_continuous)
  }
}
