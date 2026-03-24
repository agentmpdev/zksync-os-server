use proc_macro::TokenStream;
use quote::quote;
use syn::parse::{Parse, ParseStream};
use syn::{
    Attribute, Data, DeriveInput, Expr, Field, Fields, Ident, LitStr, Result, Token, Type,
    TypePath, parse_macro_input, spanned::Spanned,
};

#[derive(Default)]
struct StructAttrs {
    root: bool,
}

enum ValidatorKind {
    RequiredIfMain,
    RequiredIfExternal,
    Custom {
        predicate: Box<Expr>,
        message: Box<Expr>,
    },
}

#[derive(Default)]
struct FieldAttrs {
    path: Option<LitStr>,
    nested: bool,
    validators: Vec<ValidatorKind>,
    async_validators: Vec<Expr>,
}

struct ValidationArgs {
    predicate: Expr,
    message: Expr,
}

impl Parse for ValidationArgs {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let predicate = input.parse()?;
        input.parse::<Token![,]>()?;
        let message = input.parse()?;
        Ok(Self { predicate, message })
    }
}

fn parse_struct_attrs(attrs: &[Attribute]) -> Result<StructAttrs> {
    let mut parsed = StructAttrs::default();
    for attr in attrs {
        if !attr.path().is_ident("config_validate") {
            continue;
        }
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("root") {
                parsed.root = true;
                Ok(())
            } else {
                Err(meta.error("unsupported `config_validate` attribute"))
            }
        })?;
    }
    Ok(parsed)
}

fn parse_field_attrs(field: &Field) -> Result<FieldAttrs> {
    let mut parsed = FieldAttrs::default();
    for attr in &field.attrs {
        if !attr.path().is_ident("config_validate") {
            continue;
        }
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("path") {
                parsed.path = Some(meta.value()?.parse()?);
                Ok(())
            } else if meta.path.is_ident("nested") {
                parsed.nested = true;
                Ok(())
            } else if meta.path.is_ident("required_if_main") {
                ensure_option_field(field, "required_if_main")?;
                parsed.validators.push(ValidatorKind::RequiredIfMain);
                Ok(())
            } else if meta.path.is_ident("required_if_external") {
                ensure_option_field(field, "required_if_external")?;
                parsed.validators.push(ValidatorKind::RequiredIfExternal);
                Ok(())
            } else if meta.path.is_ident("validate") {
                let args = meta.input.parse::<ParenValidationArgs>()?;
                parsed.validators.push(ValidatorKind::Custom {
                    predicate: Box::new(args.predicate),
                    message: Box::new(args.message),
                });
                Ok(())
            } else if meta.path.is_ident("async_validate") {
                let args = meta.input.parse::<ParenExpr>()?;
                parsed.async_validators.push(args.expr);
                Ok(())
            } else {
                Err(meta.error("unsupported `config_validate` field attribute"))
            }
        })?;
    }
    Ok(parsed)
}

fn ensure_option_field(field: &Field, attr_name: &str) -> Result<()> {
    if option_inner_type(&field.ty).is_none() {
        Err(syn::Error::new(
            field.ty.span(),
            format!("`{attr_name}` can only be used on Option fields"),
        ))
    } else {
        Ok(())
    }
}

fn option_inner_type(ty: &Type) -> Option<&Type> {
    let Type::Path(TypePath { path, .. }) = ty else {
        return None;
    };
    let segment = path.segments.last()?;
    if segment.ident != "Option" {
        return None;
    }
    let syn::PathArguments::AngleBracketed(args) = &segment.arguments else {
        return None;
    };
    if args.args.len() != 1 {
        return None;
    }
    match &args.args[0] {
        syn::GenericArgument::Type(inner) => Some(inner),
        _ => None,
    }
}

struct ParenExpr {
    expr: Expr,
}

impl Parse for ParenExpr {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let content;
        syn::parenthesized!(content in input);
        Ok(Self {
            expr: content.parse()?,
        })
    }
}

struct ParenValidationArgs {
    predicate: Expr,
    message: Expr,
}

impl Parse for ParenValidationArgs {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let content;
        syn::parenthesized!(content in input);
        let args = content.parse::<ValidationArgs>()?;
        Ok(Self {
            predicate: args.predicate,
            message: args.message,
        })
    }
}

#[proc_macro_derive(ConfigValidate, attributes(config_validate))]
pub fn derive_config_validate(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match expand(input) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.into_compile_error().into(),
    }
}

fn expand(input: DeriveInput) -> Result<proc_macro2::TokenStream> {
    let name = input.ident;
    let struct_attrs = parse_struct_attrs(&input.attrs)?;
    let Data::Struct(data) = input.data else {
        return Err(syn::Error::new(
            name.span(),
            "`ConfigValidate` only supports structs",
        ));
    };
    let Fields::Named(fields) = data.fields else {
        return Err(syn::Error::new(
            name.span(),
            "`ConfigValidate` requires named fields",
        ));
    };

    let mut field_validations = Vec::new();
    let mut nested_validations = Vec::new();
    let mut async_field_validations = Vec::new();

    for field in &fields.named {
        let field_ident = field.ident.as_ref().unwrap();
        let field_attrs = parse_field_attrs(field)?;
        let default_path = default_path_segment(field_ident);
        let path_segment = field_attrs.path.unwrap_or(default_path);

        for validator in field_attrs.validators {
            let (condition, message) = match validator {
                ValidatorKind::RequiredIfMain => (
                    quote! { !root.general_config.node_role.is_main() || self.#field_ident.is_some() },
                    quote! { "is required when `general.node_role=main`" },
                ),
                ValidatorKind::RequiredIfExternal => (
                    quote! { !root.general_config.node_role.is_external() || self.#field_ident.is_some() },
                    quote! { "is required when `general.node_role=external`" },
                ),
                ValidatorKind::Custom { predicate, message } => (
                    quote! { (#predicate)(root, &self.#field_ident) },
                    quote! { #message },
                ),
            };
            field_validations.push(quote! {
                if !(#condition) {
                    let path = crate::config::join_validation_path(prefix, #path_segment);
                    errors.push(format!("`{}` {}", path, #message));
                }
            });
        }

        if field_attrs.nested {
            nested_validations.push(nested_validation(field, field_ident, &path_segment));
        }

        for validator in field_attrs.async_validators {
            async_field_validations.push(quote! {
                (#validator)(self, &self.#field_ident).await?;
            });
        }
    }

    let validator_impl = quote! {
        impl crate::config::ConditionalConfigValidator for #name {
            fn validate_conditional(
                &self,
                root: &crate::config::Config,
                errors: &mut ::std::vec::Vec<::std::string::String>,
                prefix: &str,
            ) {
                #(#field_validations)*
                #(#nested_validations)*
            }
        }
    };

    if !struct_attrs.root && !async_field_validations.is_empty() {
        return Err(syn::Error::new(
            name.span(),
            "`async_validate` can only be used on the root config struct",
        ));
    }

    let validate_method = if struct_attrs.root {
        quote! {
            impl #name {
                pub async fn validate(&self) -> ::anyhow::Result<()> {
                    let mut errors = ::std::vec::Vec::new();
                    <Self as crate::config::ConditionalConfigValidator>::validate_conditional(
                        self,
                        self,
                        &mut errors,
                        "",
                    );
                    if !errors.is_empty() {
                        ::anyhow::bail!(crate::config::format_validation_errors("invalid config", &errors));
                    }
                    #(#async_field_validations)*
                    Ok(())
                }
            }
        }
    } else {
        proc_macro2::TokenStream::new()
    };

    Ok(quote! {
        #validator_impl
        #validate_method
    })
}

fn default_path_segment(field_ident: &Ident) -> LitStr {
    let field_name = field_ident.to_string();
    let path = field_name
        .strip_suffix("_config")
        .unwrap_or(&field_name)
        .to_owned();
    LitStr::new(&path, field_ident.span())
}

fn nested_validation(
    field: &Field,
    field_ident: &Ident,
    path_segment: &LitStr,
) -> proc_macro2::TokenStream {
    if option_inner_type(&field.ty).is_some() {
        quote! {
            if let Some(value) = &self.#field_ident {
                let child_prefix = crate::config::join_validation_path(prefix, #path_segment);
                crate::config::ConditionalConfigValidator::validate_conditional(
                    value,
                    root,
                    errors,
                    &child_prefix,
                );
            }
        }
    } else {
        quote! {
            let child_prefix = crate::config::join_validation_path(prefix, #path_segment);
            crate::config::ConditionalConfigValidator::validate_conditional(
                &self.#field_ident,
                root,
                errors,
                &child_prefix,
            );
        }
    }
}
