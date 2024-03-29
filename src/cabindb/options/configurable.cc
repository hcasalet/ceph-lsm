// Copyright (c) 2011-present, Facebook, Inc. All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "include/cabindb/configurable.h"

#include "logging/logging.h"
#include "options/configurable_helper.h"
#include "options/options_helper.h"
#include "include/cabindb/customizable.h"
#include "include/cabindb/status.h"
#include "include/cabindb/utilities/object_registry.h"
#include "include/cabindb/utilities/options_type.h"
#include "util/coding.h"
#include "util/string_util.h"

namespace CABINDB_NAMESPACE {

void ConfigurableHelper::RegisterOptions(
    Configurable& configurable, const std::string& name, void* opt_ptr,
    const std::unordered_map<std::string, OptionTypeInfo>* type_map) {
  Configurable::RegisteredOptions opts;
  opts.name = name;
#ifndef CABINDB_LITE
  opts.type_map = type_map;
#else
  (void)type_map;
#endif  // CABINDB_LITE
  opts.opt_ptr = opt_ptr;
  configurable.options_.emplace_back(opts);
}

//*************************************************************************
//
//       Methods for Initializing and Validating Configurable Objects
//
//*************************************************************************

Status Configurable::PrepareOptions(const ConfigOptions& opts) {
  Status status = Status::OK();
#ifndef CABINDB_LITE
  for (auto opt_iter : options_) {
    for (auto map_iter : *(opt_iter.type_map)) {
      auto& opt_info = map_iter.second;
      if (!opt_info.IsDeprecated() && !opt_info.IsAlias() &&
          opt_info.IsConfigurable()) {
        if (!opt_info.IsEnabled(OptionTypeFlags::kDontPrepare)) {
          Configurable* config =
              opt_info.AsRawPointer<Configurable>(opt_iter.opt_ptr);
          if (config != nullptr) {
            status = config->PrepareOptions(opts);
            if (!status.ok()) {
              return status;
            }
          }
        }
      }
    }
  }
#else
  (void)opts;
#endif  // CABINDB_LITE
  if (status.ok()) {
    prepared_ = true;
  }
  return status;
}

Status Configurable::ValidateOptions(const DBOptions& db_opts,
                                     const ColumnFamilyOptions& cf_opts) const {
  Status status;
#ifndef CABINDB_LITE
  for (auto opt_iter : options_) {
    for (auto map_iter : *(opt_iter.type_map)) {
      auto& opt_info = map_iter.second;
      if (!opt_info.IsDeprecated() && !opt_info.IsAlias()) {
        if (opt_info.IsConfigurable()) {
          const Configurable* config =
              opt_info.AsRawPointer<Configurable>(opt_iter.opt_ptr);
          if (config != nullptr) {
            status = config->ValidateOptions(db_opts, cf_opts);
          } else if (!opt_info.CanBeNull()) {
            status =
                Status::NotFound("Missing configurable object", map_iter.first);
          }
          if (!status.ok()) {
            return status;
          }
        }
      }
    }
  }
#else
  (void)db_opts;
  (void)cf_opts;
#endif  // CABINDB_LITE
  return status;
}

/*********************************************************************************/
/*                                                                               */
/*       Methods for Retrieving Options from Configurables */
/*                                                                               */
/*********************************************************************************/

const void* Configurable::GetOptionsPtr(const std::string& name) const {
  for (auto o : options_) {
    if (o.name == name) {
      return o.opt_ptr;
    }
  }
  return nullptr;
}

std::string Configurable::GetOptionName(const std::string& opt_name) const {
  return opt_name;
}

#ifndef CABINDB_LITE
const OptionTypeInfo* ConfigurableHelper::FindOption(
    const std::vector<Configurable::RegisteredOptions>& options,
    const std::string& short_name, std::string* opt_name, void** opt_ptr) {
  for (auto iter : options) {
    const auto opt_info =
        OptionTypeInfo::Find(short_name, *(iter.type_map), opt_name);
    if (opt_info != nullptr) {
      *opt_ptr = iter.opt_ptr;
      return opt_info;
    }
  }
  return nullptr;
}
#endif  // CABINDB_LITE

//*************************************************************************
//
//       Methods for Configuring Options from Strings/Name-Value Pairs/Maps
//
//*************************************************************************

Status Configurable::ConfigureFromMap(
    const ConfigOptions& config_options,
    const std::unordered_map<std::string, std::string>& opts_map) {
  Status s = ConfigureFromMap(config_options, opts_map, nullptr);
  return s;
}

Status Configurable::ConfigureFromMap(
    const ConfigOptions& config_options,
    const std::unordered_map<std::string, std::string>& opts_map,
    std::unordered_map<std::string, std::string>* unused) {
  return ConfigureOptions(config_options, opts_map, unused);
}

Status Configurable::ConfigureOptions(
    const ConfigOptions& config_options,
    const std::unordered_map<std::string, std::string>& opts_map,
    std::unordered_map<std::string, std::string>* unused) {
  std::string curr_opts;
#ifndef CABINDB_LITE
  if (!config_options.ignore_unknown_options) {
    // If we are not ignoring unused, get the defaults in case we need to reset
    GetOptionString(config_options, &curr_opts).PermitUncheckedError();
  }
#endif  // CABINDB_LITE
  Status s = ConfigurableHelper::ConfigureOptions(config_options, *this,
                                                  opts_map, unused);
  if (config_options.invoke_prepare_options && s.ok()) {
    s = PrepareOptions(config_options);
  }
#ifndef CABINDB_LITE
  if (!s.ok() && !curr_opts.empty()) {
    ConfigOptions reset = config_options;
    reset.ignore_unknown_options = true;
    reset.invoke_prepare_options = true;
    // There are some options to reset from this current error
    ConfigureFromString(reset, curr_opts).PermitUncheckedError();
  }
#endif  // CABINDB_LITE
  return s;
}

Status Configurable::ParseStringOptions(const ConfigOptions& /*config_options*/,
                                        const std::string& /*opts_str*/) {
  return Status::OK();
}

Status Configurable::ConfigureFromString(const ConfigOptions& config_options,
                                         const std::string& opts_str) {
  Status s;
  if (!opts_str.empty()) {
#ifndef CABINDB_LITE
    if (opts_str.find(';') != std::string::npos ||
        opts_str.find('=') != std::string::npos) {
      std::unordered_map<std::string, std::string> opt_map;
      s = StringToMap(opts_str, &opt_map);
      if (s.ok()) {
        s = ConfigureFromMap(config_options, opt_map, nullptr);
      }
    } else {
#endif  // CABINDB_LITE
      s = ParseStringOptions(config_options, opts_str);
      if (s.ok() && config_options.invoke_prepare_options) {
        s = PrepareOptions(config_options);
      }
#ifndef CABINDB_LITE
    }
#endif  // CABINDB_LITE
  } else if (config_options.invoke_prepare_options) {
    s = PrepareOptions(config_options);
  } else {
    s = Status::OK();
  }
  return s;
}

#ifndef CABINDB_LITE
/**
 * Sets the value of the named property to the input value, returning OK on
 * succcess.
 */
Status Configurable::ConfigureOption(const ConfigOptions& config_options,
                                     const std::string& name,
                                     const std::string& value) {
  const std::string& opt_name = GetOptionName(name);
  return ConfigurableHelper::ConfigureSingleOption(config_options, *this,
                                                   opt_name, value);
}

/**
 * Looks for the named option amongst the options for this type and sets
 * the value for it to be the input value.
 * If the name was found, found_option will be set to true and the resulting
 * status should be returned.
 */

Status Configurable::ParseOption(const ConfigOptions& config_options,
                                 const OptionTypeInfo& opt_info,
                                 const std::string& opt_name,
                                 const std::string& opt_value, void* opt_ptr) {
  if (opt_info.IsMutable() || opt_info.IsConfigurable()) {
    return opt_info.Parse(config_options, opt_name, opt_value, opt_ptr);
  } else if (prepared_) {
    return Status::InvalidArgument("Option not changeable: " + opt_name);
  } else {
    return opt_info.Parse(config_options, opt_name, opt_value, opt_ptr);
  }
}

#endif  // CABINDB_LITE

Status ConfigurableHelper::ConfigureOptions(
    const ConfigOptions& config_options, Configurable& configurable,
    const std::unordered_map<std::string, std::string>& opts_map,
    std::unordered_map<std::string, std::string>* unused) {
  std::unordered_map<std::string, std::string> remaining = opts_map;
  Status s = Status::OK();
  if (!opts_map.empty()) {
#ifndef CABINDB_LITE
    for (const auto& iter : configurable.options_) {
      s = ConfigureSomeOptions(config_options, configurable, *(iter.type_map),
                               &remaining, iter.opt_ptr);
      if (remaining.empty()) {  // Are there more options left?
        break;
      } else if (!s.ok()) {
        break;
      }
    }
#else
    (void)configurable;
    if (!config_options.ignore_unknown_options) {
      s = Status::NotSupported("ConfigureFromMap not supported in LITE mode");
    }
#endif  // CABINDB_LITE
  }
  if (unused != nullptr && !remaining.empty()) {
    unused->insert(remaining.begin(), remaining.end());
  }
  if (config_options.ignore_unknown_options) {
    s = Status::OK();
  } else if (s.ok() && unused == nullptr && !remaining.empty()) {
    s = Status::NotFound("Could not find option: ", remaining.begin()->first);
  }
  return s;
}

#ifndef CABINDB_LITE
/**
 * Updates the object with the named-value property values, returning OK on
 * succcess. Any properties that were found are removed from the options list;
 * upon return only options that were not found in this opt_map remain.

 * Returns:
 * -  OK if ignore_unknown_options is set
 * - InvalidArgument, if any option was invalid
 * - NotSupported, if any option is unsupported and ignore_unsupported_options
 is OFF
 * - OK, if no option was invalid or not supported (or ignored)
 */
Status ConfigurableHelper::ConfigureSomeOptions(
    const ConfigOptions& config_options, Configurable& configurable,
    const std::unordered_map<std::string, OptionTypeInfo>& type_map,
    std::unordered_map<std::string, std::string>* options, void* opt_ptr) {
  Status result = Status::OK();  // The last non-OK result (if any)
  Status notsup = Status::OK();  // The last NotSupported result (if any)
  std::string elem_name;
  int found = 1;
  std::unordered_set<std::string> unsupported;
  // While there are unused properties and we processed at least one,
  // go through the remaining unused properties and attempt to configure them.
  while (found > 0 && !options->empty()) {
    found = 0;
    notsup = Status::OK();
    for (auto it = options->begin(); it != options->end();) {
      const std::string& opt_name = configurable.GetOptionName(it->first);
      const std::string& opt_value = it->second;
      const auto opt_info =
          OptionTypeInfo::Find(opt_name, type_map, &elem_name);
      if (opt_info == nullptr) {  // Did not find the option.  Skip it
        ++it;
      } else {
        Status s = ConfigureOption(config_options, configurable, *opt_info,
                                   opt_name, elem_name, opt_value, opt_ptr);
        if (s.IsNotFound()) {
          ++it;
        } else if (s.IsNotSupported()) {
          notsup = s;
          unsupported.insert(it->first);
          ++it;  // Skip it for now
        } else {
          found++;
          it = options->erase(it);
          if (!s.ok()) {
            result = s;
          }
        }
      }
    }  // End for all remaining options
  }    // End while found one or options remain

  // Now that we have been through the list, remove any unsupported
  for (auto u : unsupported) {
    auto it = options->find(u);
    if (it != options->end()) {
      options->erase(it);
    }
  }
  if (config_options.ignore_unknown_options) {
    if (!result.ok()) result.PermitUncheckedError();
    if (!notsup.ok()) notsup.PermitUncheckedError();
    return Status::OK();
  } else if (!result.ok()) {
    if (!notsup.ok()) notsup.PermitUncheckedError();
    return result;
  } else if (config_options.ignore_unsupported_options) {
    if (!notsup.ok()) notsup.PermitUncheckedError();
    return Status::OK();
  } else {
    return notsup;
  }
}

Status ConfigurableHelper::ConfigureSingleOption(
    const ConfigOptions& config_options, Configurable& configurable,
    const std::string& name, const std::string& value) {
  std::string opt_name;
  void* opt_ptr = nullptr;
  const auto opt_info =
      FindOption(configurable.options_, name, &opt_name, &opt_ptr);
  if (opt_info == nullptr) {
    return Status::NotFound("Could not find option: ", name);
  } else {
    return ConfigureOption(config_options, configurable, *opt_info, name,
                           opt_name, value, opt_ptr);
  }
}

Status ConfigurableHelper::ConfigureOption(
    const ConfigOptions& config_options, Configurable& configurable,
    const OptionTypeInfo& opt_info, const std::string& opt_name,
    const std::string& name, const std::string& value, void* opt_ptr) {
  if (opt_name == name) {
    return configurable.ParseOption(config_options, opt_info, opt_name, value,
                                    opt_ptr);
  } else if (opt_info.IsCustomizable() &&
             EndsWith(opt_name, ConfigurableHelper::kIdPropSuffix)) {
    return configurable.ParseOption(config_options, opt_info, name, value,
                                    opt_ptr);

  } else if (opt_info.IsCustomizable()) {
    Customizable* custom = opt_info.AsRawPointer<Customizable>(opt_ptr);
    if (value.empty()) {
      return Status::OK();
    } else if (custom == nullptr || !StartsWith(name, custom->GetId() + ".")) {
      return configurable.ParseOption(config_options, opt_info, name, value,
                                      opt_ptr);
    } else if (value.find("=") != std::string::npos) {
      return custom->ConfigureFromString(config_options, value);
    } else {
      return custom->ConfigureOption(config_options, name, value);
    }
  } else if (opt_info.IsStruct() || opt_info.IsConfigurable()) {
    return configurable.ParseOption(config_options, opt_info, name, value,
                                    opt_ptr);
  } else {
    return Status::NotFound("Could not find option: ", name);
  }
}
#endif  // CABINDB_LITE

Status ConfigurableHelper::ConfigureNewObject(
    const ConfigOptions& config_options_in, Configurable* object,
    const std::string& id, const std::string& base_opts,
    const std::unordered_map<std::string, std::string>& opts) {
  if (object != nullptr) {
    ConfigOptions config_options = config_options_in;
    config_options.invoke_prepare_options = false;
    if (!base_opts.empty()) {
#ifndef CABINDB_LITE
      // Don't run prepare options on the base, as we would do that on the
      // overlay opts instead
      Status status = object->ConfigureFromString(config_options, base_opts);
      if (!status.ok()) {
        return status;
      }
#endif  // CABINDB_LITE
    }
    if (!opts.empty()) {
      return object->ConfigureFromMap(config_options, opts);
    }
  } else if (!opts.empty()) {  // No object but no map.  This is OK
    return Status::InvalidArgument("Cannot configure null object ", id);
  }
  return Status::OK();
}

//*******************************************************************************
//
//       Methods for Converting Options into strings
//
//*******************************************************************************

Status Configurable::GetOptionString(const ConfigOptions& config_options,
                                     std::string* result) const {
  assert(result);
  result->clear();
#ifndef CABINDB_LITE
  return ConfigurableHelper::SerializeOptions(config_options, *this, "",
                                              result);
#else
  (void)config_options;
  return Status::NotSupported("GetOptionString not supported in LITE mode");
#endif  // CABINDB_LITE
}

#ifndef CABINDB_LITE
std::string Configurable::ToString(const ConfigOptions& config_options,
                                   const std::string& prefix) const {
  std::string result = SerializeOptions(config_options, prefix);
  if (result.empty() || result.find('=') == std::string::npos) {
    return result;
  } else {
    return "{" + result + "}";
  }
}

std::string Configurable::SerializeOptions(const ConfigOptions& config_options,
                                           const std::string& header) const {
  std::string result;
  Status s = ConfigurableHelper::SerializeOptions(config_options, *this, header,
                                                  &result);
  assert(s.ok());
  return result;
}

Status Configurable::GetOption(const ConfigOptions& config_options,
                               const std::string& name,
                               std::string* value) const {
  return ConfigurableHelper::GetOption(config_options, *this,
                                       GetOptionName(name), value);
}

Status ConfigurableHelper::GetOption(const ConfigOptions& config_options,
                                     const Configurable& configurable,
                                     const std::string& short_name,
                                     std::string* value) {
  // Look for option directly
  assert(value);
  value->clear();

  std::string opt_name;
  void* opt_ptr = nullptr;
  const auto opt_info =
      FindOption(configurable.options_, short_name, &opt_name, &opt_ptr);
  if (opt_info != nullptr) {
    ConfigOptions embedded = config_options;
    embedded.delimiter = ";";
    if (short_name == opt_name) {
      return opt_info->Serialize(embedded, opt_name, opt_ptr, value);
    } else if (opt_info->IsStruct()) {
      return opt_info->Serialize(embedded, opt_name, opt_ptr, value);
    } else if (opt_info->IsConfigurable()) {
      auto const* config = opt_info->AsRawPointer<Configurable>(opt_ptr);
      if (config != nullptr) {
        return config->GetOption(embedded, opt_name, value);
      }
    }
  }
  return Status::NotFound("Cannot find option: ", short_name);
}

Status ConfigurableHelper::SerializeOptions(const ConfigOptions& config_options,
                                            const Configurable& configurable,
                                            const std::string& prefix,
                                            std::string* result) {
  assert(result);
  for (auto const& opt_iter : configurable.options_) {
    for (const auto& map_iter : *(opt_iter.type_map)) {
      const auto& opt_name = map_iter.first;
      const auto& opt_info = map_iter.second;
      if (opt_info.ShouldSerialize()) {
        std::string value;
        Status s = opt_info.Serialize(config_options, prefix + opt_name,
                                      opt_iter.opt_ptr, &value);
        if (!s.ok()) {
          return s;
        } else if (!value.empty()) {
          // <prefix><opt_name>=<value><delimiter>
          result->append(prefix + opt_name + "=" + value +
                         config_options.delimiter);
        }
      }
    }
  }
  return Status::OK();
}
#endif  // CABINDB_LITE

//********************************************************************************
//
// Methods for listing the options from Configurables
//
//********************************************************************************
#ifndef CABINDB_LITE
Status Configurable::GetOptionNames(
    const ConfigOptions& config_options,
    std::unordered_set<std::string>* result) const {
  assert(result);
  return ConfigurableHelper::ListOptions(config_options, *this, "", result);
}

Status ConfigurableHelper::ListOptions(
    const ConfigOptions& /*config_options*/, const Configurable& configurable,
    const std::string& prefix, std::unordered_set<std::string>* result) {
  Status status;
  for (auto const& opt_iter : configurable.options_) {
    for (const auto& map_iter : *(opt_iter.type_map)) {
      const auto& opt_name = map_iter.first;
      const auto& opt_info = map_iter.second;
      // If the option is no longer used in cabindb and marked as deprecated,
      // we skip it in the serialization.
      if (!opt_info.IsDeprecated() && !opt_info.IsAlias()) {
        result->emplace(prefix + opt_name);
      }
    }
  }
  return status;
}
#endif  // CABINDB_LITE

//*******************************************************************************
//
//       Methods for Comparing Configurables
//
//*******************************************************************************

bool Configurable::AreEquivalent(const ConfigOptions& config_options,
                                 const Configurable* other,
                                 std::string* name) const {
  assert(name);
  name->clear();
  if (this == other || config_options.IsCheckDisabled()) {
    return true;
  } else if (other != nullptr) {
#ifndef CABINDB_LITE
    return ConfigurableHelper::AreEquivalent(config_options, *this, *other,
                                             name);
#else
    return true;
#endif  // CABINDB_LITE
  } else {
    return false;
  }
}

#ifndef CABINDB_LITE
bool Configurable::OptionsAreEqual(const ConfigOptions& config_options,
                                   const OptionTypeInfo& opt_info,
                                   const std::string& opt_name,
                                   const void* const this_ptr,
                                   const void* const that_ptr,
                                   std::string* mismatch) const {
  if (opt_info.AreEqual(config_options, opt_name, this_ptr, that_ptr,
                        mismatch)) {
    return true;
  } else if (opt_info.AreEqualByName(config_options, opt_name, this_ptr,
                                     that_ptr)) {
    *mismatch = "";
    return true;
  } else {
    return false;
  }
}

bool ConfigurableHelper::AreEquivalent(const ConfigOptions& config_options,
                                       const Configurable& this_one,
                                       const Configurable& that_one,
                                       std::string* mismatch) {
  assert(mismatch != nullptr);
  for (auto const& o : this_one.options_) {
    const auto this_offset = this_one.GetOptionsPtr(o.name);
    const auto that_offset = that_one.GetOptionsPtr(o.name);
    if (this_offset != that_offset) {
      if (this_offset == nullptr || that_offset == nullptr) {
        return false;
      } else {
        for (const auto& map_iter : *(o.type_map)) {
          if (config_options.IsCheckEnabled(map_iter.second.GetSanityLevel()) &&
              !this_one.OptionsAreEqual(config_options, map_iter.second,
                                        map_iter.first, this_offset,
                                        that_offset, mismatch)) {
            return false;
          }
        }
      }
    }
  }
  return true;
}
#endif  // CABINDB_LITE

Status ConfigurableHelper::GetOptionsMap(
    const std::string& value, std::string* id,
    std::unordered_map<std::string, std::string>* props) {
  return GetOptionsMap(value, "", id, props);
}

Status ConfigurableHelper::GetOptionsMap(
    const std::string& value, const std::string& default_id, std::string* id,
    std::unordered_map<std::string, std::string>* props) {
  assert(id);
  assert(props);
  Status status;
  if (value.empty() || value == kNullptrString) {
    *id = default_id;
  } else if (value.find('=') == std::string::npos) {
    *id = value;
#ifndef CABINDB_LITE
  } else {
    status = StringToMap(value, props);
    if (status.ok()) {
      auto iter = props->find(ConfigurableHelper::kIdPropName);
      if (iter != props->end()) {
        *id = iter->second;
        props->erase(iter);
      } else if (default_id.empty()) {  // Should this be an error??
        status = Status::InvalidArgument("Name property is missing");
      } else {
        *id = default_id;
      }
    }
#else
  } else {
    *id = value;
    props->clear();
#endif
  }
  return status;
}
}  // namespace CABINDB_NAMESPACE
