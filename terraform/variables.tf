variable "app_name" {
    default = "swodlr"
    type = string
}

variable "service_name" {
    default = "raster-create"
    type = string
}

variable "default_tags" {
    type = map(string)
    default = {}
}

variable "stage" {
    type = string
}

variable "region" {
    type = string
}

variable "cmr_graphql_endpoint" {
    type = string
}

variable "edl_token" {
    type = string
}

variable "pixc_concept_id" {
    type = string
}

variable "pixcvec_concept_id" {
    type = string
}

variable "xdf_orbit_concept_id" {
    type = string
}

variable "sds_pcm_release_tag" {
    type = string
}

variable "sds_host" {
    type = string
    sensitive = true
}

variable "sds_username" {
    type = string
    sensitive = true
}

variable "sds_password" {
    type = string
    sensitive = true
}

variable "sds_ca_cert_path" {
    type = string
    default = "/etc/ssl/certs/JPLICA.Root.pem"
}

variable "sds_grq_es_index" {
    type = string
    default = "grq"
}

variable "sds_grq_es_path" {
    type = string
    default = "/grq_es"
}

variable "sds_submit_max_attempts" {
    type = number
    default = 5
}

variable "sds_submit_timeout" {
    type = number
    default = 20
}

variable "update_max_attempts" {
    type = number
    default = 5
}

variable "sds_rs_bucket" {
    type = string
}

variable "publish_bucket" {
    type = string
}

variable "log_level" {
    type = string
    default = "INFO"
}
